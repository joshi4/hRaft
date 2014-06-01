{-# LANGUAGE DeriveGeneric #-}
import Network.Socket
import Control.Monad
import Control.Concurrent (forkIO, threadDelay)
import Control.Exception
import Data.List 
import System.Environment (getArgs)
import qualified Data.Map.Strict as Map
import qualified Network.Socket.ByteString as NBS 
import qualified Network.Socket as NBS hiding (send, sendTo, recv, recvFrom )
import qualified Data.ByteString as BS
import GHC.Generics
import Data.Serialize
import System.Timeout (timeout)
import System.Random hiding (split) -- ^ for election timer 
type Term = Int
type Host = String
type PortNum = String 


-- TOP LEVEL DECLARATION FOR TIME OUT --


---- # GLOBAL CONSTANTS # ----

electionTimerLL = 150000 -- ^ Lower Limit for election Time out 
electionTimerUL = 500000 -- ^ Upper Limit for election Time out
heartbeatTimer = 20000 -- ^ Heartbeat timer every 20 ms 

majority = 2 -- ^ Amount of votes a candidate needs to become a leader.

---- # GLOBAL CONSTANTS END  # ----

type Log = [LogBlock]

data LogBlock = LogBlock {
  logterm :: Term, 
  instruction :: BS.ByteString 
  } deriving (Show, Generic) 

-- functions to get last index and most recent term in the log. 
getLastLogIndex :: Log  -> Int
getLastLogIndex l = let len = (length l )
                    in
                     if len == 0 then len else len - 1 

getLastLogTerm :: Log  -> Term
getLastLogTerm l = let index = getLastLogIndex l
                   in
                    if index == 0 then 0  else logterm  (l !! index ) 


data Message = MRequestVote RequestVote
             | MRequestVoteReply RequestVoteReply
             | MAppendEntries AppendEntries
             | MHeartbeat AppendEntries
             | Empty 
               deriving (Show, Generic)


data AppendEntries = AppendEntries {
  leaderTerm :: Term,
  leaderId :: SockAddr, -- ^ so follower can redirect the clients
  prevLogIndex :: Int,
  prevLogTerm :: Term,
  entries :: Log -- ^ EMPTY for heartbeat
  } deriving (Show, Generic )


data RequestVote = RequestVote {
  cTerm :: Term
  ,cId :: String
  ,lastLogIndex :: Int
  ,lastLogTerm :: Term 
  } deriving (Show, Generic)


data RequestVoteReply = RequestVoteReply {
  reqVRterm :: Term,
  voteGranted :: Bool 
  } deriving (Show, Generic)



data LeaderState = LeaderState {
  nextIndexDict :: Map.Map SockAddr Int -- ^ Index of the next log entry leader will send to key. ( init to next expected index in log for leader.)
  ,matchIndexDict :: Map.Map SockAddr Int -- ^ For each server index of highest log entry known to be replicated on the server. 
  } deriving (Show, Generic, Eq)


type Configuration =   Map.Map String SockAddr
data Role = Follower | Leader LeaderState | Candidate deriving (Show, Generic, Eq )


-- This is going to tell me the state of the particular particpant in the
-- consensus. Its Role (Leader, Follower, Candidate etc) The state
-- that is required
data RaftState = Raft {
  leaderID :: Maybe SockAddr  -- ^ Id of the server, still not decided on cloudhaskell or UDP 
  ,currentTerm :: Term
  ,role :: Role
  ,myNode :: String
  ,getlog :: Log
  ,commitIndex :: Int -- ^ index of the latest commited entry in the log, in this case its the same as lastAppliesd. 
  ,votedFor :: Maybe SockAddr -- ^This is the socketAddress converted to a string
  ,participantsMap :: Configuration
  ,electionTimeOut :: Int 
  ,lastApplied :: Int -- ^ Index of highest log entry applied to statemachine 
  } deriving (Show, Generic)



-- Instances to serialize stuff so it can be decoded, encoded 
instance Serialize SockAddr where
  put sa = let str = show sa  -- ^ convert SAddr to String 
           in put str
  -- ^ using the constructor we can convert String to SockAddr
  get = (get :: Get [Char])  >>= (\str  -> return $ (SockAddrUnix str))  


instance Serialize RaftState
instance Serialize RequestVote
instance Serialize RequestVoteReply
instance Serialize Message
instance Serialize Role 
instance Serialize LogBlock
instance Serialize AppendEntries
instance Serialize LeaderState


--- END OF IMPORTS AND DATA DECLARATIONS ---- 

incrementTerm :: Term  -> Term
incrementTerm t = t+1


getBindAddr :: String -> IO SockAddr
getBindAddr port = do
 ai:_ <- getAddrInfo (Just defai) Nothing (Just port)
 return $ addrAddress ai
 where defai = defaultHints { addrFlags = [AI_PASSIVE]
                            , addrFamily = AF_INET }


getSockAddr :: (Host , PortNum) -> IO SockAddr
getSockAddr (name , port) = do
 ai:_ <- getAddrInfo (Just defai) (Just name) (Just port)
 return $ addrAddress ai
 where defai = defaultHints { addrFamily = AF_INET
                            , addrSocketType = Datagram }


{-   INITIALIZATION CODE   -}


initConfiguration :: IO Configuration
initConfiguration = do
  sAddr  <- mapM getSockAddr [("localhost", "8003"), ("localhost", "8001"), ("localhost", "8002")]
  let dictList = zip ["1","2", "3"] sAddr
  return $ Map.fromList dictList


initRaftState :: Configuration  -> String -> RaftState
initRaftState config node = Raft {
  leaderID = Nothing
  ,currentTerm = 0
  ,role = Follower
  ,votedFor = Nothing
  ,myNode = node
  ,getlog = []
  ,commitIndex = 0 
  ,electionTimeOut = 0 -- ^ Keeping ths a pure function, check initSystem for fix.
  ,participantsMap = config
  ,lastApplied = 0 
  }

initSystem :: String  -> IO RaftState
initSystem node = do
  configuration  <- initConfiguration
  electionTimer  <- getStdRandom (randomR (electionTimerLL, electionTimerUL )) 
  rs  <- return $ initRaftState configuration node
  return $ rs {electionTimeOut = electionTimer}
   
{-   INITIALIZATION CODE ENDS HERE    -}

sendMessageTillSuccess :: Socket  -> SockAddr  -> IO ()
sendMessageTillSuccess s sa  = do
  bytesSent  <- NBS.sendTo s (encode 'A') sa
  putStrLn $ show bytesSent
  if bytesSent > 0 then return () else sendMessageTillSuccess s sa 

-- broadcasts the given message to all other nodes. 
broadCastMessage :: RaftState  -> Message  -> IO ()
broadCastMessage raft msg = do
  putStrLn "Entered braodcast"
  let peerList = getPeerList raft
      mySocketAddr = getMySockAddr raft 
  void $ mapM (\(k,peer) ->   do -- removed the forkIO 
                  sendMessage msg mySocketAddr peer
              ) peerList 

-- Utility function that is used to get list of peers
-- does not include myself.   
getPeerList :: RaftState  -> [(String, SockAddr)]
getPeerList raft = (Map.toList config) \\ [(self, mySocketAddr)]
  where
    config = participantsMap raft
    self = myNode raft
    mySocketAddr = getMySockAddr raft
    
           

-- Abstracted away the check " candidate is atleast as up to date as server it is requesting for vote"
candidateUpToDateCheck :: RaftState  -> RequestVote -> SockAddr  -> (Message, RaftState)
candidateUpToDateCheck raft reqV peer  = let myTerm = currentTerm raft
                                             newReqVT = RequestVoteReply myTerm True
                                             newReqVF = RequestVoteReply myTerm False
                                             myLogLastTerm = getLastLogTerm $ getlog raft -- raft and raft' have same log. 
                                             myLogLastIndex = getLastLogIndex $ getlog raft -- same reasoning here. 
                                             cLogLastTerm = lastLogTerm reqV
                                             cLogLastIndex = lastLogIndex reqV                             
                                             raft' = raft {votedFor = Just peer}  -- ^ Update votedFor field.
                                             check _myTerm _cTerm | _myTerm == _cTerm = if cLogLastIndex >= myLogLastIndex
                                                                                    then ((MRequestVoteReply newReqVT), raft')
                                                                                    else ((MRequestVoteReply newReqVF ),raft )
                                                                | _myTerm > _cTerm = ((MRequestVoteReply newReqVF) , raft)
                                                                | otherwise = ((MRequestVoteReply newReqVT) , raft)
                                         in                                                                                       
                                            check myLogLastTerm cLogLastTerm 
                                            
-- If candidate's Term is < current Term : straight reject.
-- if candidate term is > currentTerm : perform uptodate check on logs to decide vote
-- if candidate term is equal and votedfor is Nothing perform upToDateCheck on the logs
-- otherwise reject.                                             
handleRequestVote :: RaftState  -> RequestVote -> SockAddr  -> (Message, RaftState)
handleRequestVote raft reqV peer
  | candidateTerm < myTerm  = rejectCandidate -- ^ sending more upto date term for candidate
  | candidateTerm  > myTerm  =  candidateUpToDateCheck (raft {currentTerm = candidateTerm }) reqV peer -- ^ already updated the term
  | (votedFor raft) == Nothing = candidateUpToDateCheck raft reqV peer
  | otherwise = rejectCandidate 
  where
    candidateTerm = cTerm reqV
    myTerm = currentTerm raft
    rejectCandidate = ( MRequestVoteReply ( RequestVoteReply myTerm False) , raft )



-- Where it receives all the different kinds of messages a follower can receive.
-- Heartbeat, AppenEntries, RequetVote
receiveMsgAsFollower :: RaftState ->  IO (Maybe ( (Message, RaftState), SockAddr) )
receiveMsgAsFollower raft = receiveMsg' ( participantsMap raft)
                  (myNode raft)
                  (electionTimeOut raft )
  where
    receiveMsg' config k timer = do
      defAddr  <- getBindAddr "0"
      putStrLn $ "Entered receiveMsg" ++ show k
      let recvAddr = Map.findWithDefault defAddr  k config
      bracket (socket AF_INET Datagram 0) sClose $ \s -> do
        bind s recvAddr
        result <- timeout timer (NBS.recvFrom s 0x10000) -- ^ Times out if it doesn't get message in time
        case result of
          Nothing  -> return $ Nothing -- ^ we timed out. 
          (Just (buf, peer))  ->  case decode buf of      -- ^ if there is a message ; need to figure out which one it is.       
            Left err  ->  return $ Nothing -- ^ ill formated message ; couldn't decoded it. 
            Right (MRequestVote reqV)  -> putStrLn ("Received message from candidate " ++  (show peer)) >> return (Just (handleRequestVote raft reqV peer , peer) ) -- ^ RequestToVote message
            Right (MHeartbeat _) -> putStrLn ("Received heartbeat form leader " ++ show peer ) >> return ( Just ( (Empty, raft) , peer )) 
            Right _  ->  return $ Nothing
          

    
-- Simple version , not retrying if failed: i:e when peer is down/shutoff.   
sendMessage :: Message  -> SockAddr -> SockAddr  -> IO ()
sendMessage msg myaddr peer = bracket (socket AF_INET Datagram 0) sClose
                       (\s  -> do                           
                           bind s myaddr -- ^ don't bind it to peer but bind it to your own address.
                           putStrLn "Finished binding"
                           void $ NBS.sendTo s (encode msg) peer  -- ^ May need some mechanism to retry if it has failed.
                           putStrLn "end of sendMessage"
                       )



getMySockAddr :: RaftState  -> SockAddr
getMySockAddr raft = let myid = myNode raft
                         config = participantsMap raft
                     in
                      case Map.lookup myid config of
                        Nothing  -> undefined -- ^ Error should not happen 
                        (Just x ) -> x 

runAsFollower :: RaftState  -> IO ()
runAsFollower raft = do
  result  <- receiveMsgAsFollower raft 
  let myaddr = getMySockAddr raft 
  case result of
    (Just ( (msg, raft'), peer) )  -> case msg of
      Empty  -> runAsFollower raft' 
      (MRequestVoteReply _)  ->  sendMessage msg myaddr  peer >> runAsFollower raft' -- ^ candidate has been given the reply. 
    Nothing  -> (putStrLn "timed out") >> runSystem (changeRoleToCandidate raft)


changeRoleToCandidate :: RaftState  -> RaftState
changeRoleToCandidate raft = raft { role = Candidate
                                  ,votedFor = Nothing
                                  }

changeRoleToLeader :: RaftState  -> RaftState
changeRoleToLeader raft = raft { role = Leader $ initLeaderState }
  where
    extractSockAddr :: [(String, SockAddr)] -> [SockAddr]
    extractSockAddr xs = [sa | (_,sa) <- xs ]
    defNextIndex = getLastLogIndex $ getlog raft
    defMatchIndex = 0
    filteredPeerList = (extractSockAddr $ getPeerList raft)
    nextIndexList = zip  filteredPeerList (cycle [defNextIndex])
    matchIndexList = zip  filteredPeerList  (cycle [defMatchIndex])
    initLeaderState = LeaderState { nextIndexDict = Map.fromList nextIndexList
                                  ,matchIndexDict = Map.fromList matchIndexList
                                  }

createHeartbeat :: RaftState -> AppendEntries
createHeartbeat raft = AppendEntries {
  leaderTerm = currentTerm raft 
  ,leaderId = getMySockAddr raft
  ,prevLogIndex = ( getLastLogIndex $ getlog raft) - 1 -- ^ so if this vlaue is -1 then we know log is empty. 
  ,prevLogTerm = getLastLogTerm $ getlog raft 
  ,entries = []
  }


-- 
runAsLeader :: RaftState  -> IO ()
runAsLeader raft = do
	putStrLn " I am the leader now "
        let heartbeat =  MHeartbeat $ createHeartbeat raft
        newByteString  <- timeout heartbeatTimer $ listenToClient raft
        case newByteString of
          Nothing  -> broadCastMessage raft heartbeat
          (Just buf)  -> do
                         putStrLn $ "Received message" ++ show (decode buf :: Either String String)
                         broadCastMessage raft heartbeat
        putStrLn "Sending heartbeat"
   	runAsLeader raft 



-- listenToClient listens on a special port till time runs out
-- if in tht time it has received a new message from client then
-- that is returned if not then we will block on recvFrom and timer will timeout
listenToClient :: RaftState  -> IO BS.ByteString
listenToClient raft  = do
  splSocketAddr  <- getSockAddr ("localhost", "8888")
  bracket (socket AF_INET Datagram 0) sClose (\s  -> do
                                                 bind s splSocketAddr
                                                 (buf, _) <- NBS.recvFrom s 0x10000
                                                 return buf 
                                             )

      

runAsCandidate :: RaftState  ->  IO ()
runAsCandidate raft' = do
  putStrLn "starting Elections again, I'm a candidate"
  raft  <- return $ raft' {currentTerm = incrementTerm $ currentTerm raft' } -- ^ increment term everytime this function is run. 
  let currTerm = currentTerm raft
      id = myNode raft
      votes = 1 -- ^ we've voted for ourselves, automatically 
      msg = MRequestVote $ RequestVote {cTerm = currTerm
                                       ,cId = id
                                       ,lastLogIndex = getLastLogIndex $ getlog raft
                                       ,lastLogTerm =  getLastLogTerm $ getlog raft
                                       }

  putStrLn "Before broadcast" 
  broadCastMessage raft msg -- ^ this will fork threads and send out the message to each of the other servers.
  -- return type is IO (Maybe (IO (Bool, RaftState)))
  putStrLn "Before tiem out"
  decisionIO  <- timeout (electionTimeOut raft) (tallyVotes raft votes)  -- ^ accept incoming replies to me and tally the votes.
  case decisionIO of
    Nothing  -> putStrLn "Timed Out " >> runAsCandidate raft -- ^ Timed out no leader from my perspective start as candidate again.
    (Just decision) -> case decision of
      (True, raftS)  -> runAsLeader $ changeRoleToLeader raftS
      (False, raftS)  -> if role raftS == Follower then runAsFollower raftS else runAsCandidate raftS -- ^ could be that no one wins election
      
      
    




-- Accepts incoming messages to candidate; need to handle case when another leader
-- sends it a heartbeat.   
tallyVotes :: RaftState  -> Int  -> IO (Bool, RaftState ) 
tallyVotes raft voteCount 
  | voteCount >= majority = return (True , raft ) 
  | otherwise = case Map.lookup (myNode raft) (participantsMap raft) of
                     Nothing  -> putStrLn "Error! you have node that's not in the config file" >> return (False, raft)   -- ^ not allowed to have something that's not in teh config file
                     (Just recvAddr )  -> bracket (socket AF_INET Datagram 0 ) (\s  -> do
                                                                                   bool  <- NBS.isBound s
                                                                                   if bool then sClose s else return ()
                                                                               ) $ \s  ->  do
                       
                       bind s recvAddr `onException` (putStrLn $ (show recvAddr))
                       (buf, peer) <-  NBS.recvFrom s 0x10000
                       case decode buf  of
                         Left err  ->  return $ (False, raft) 
                         Right (MRequestVoteReply reply)  -> case reply of 
                           (RequestVoteReply _ True )  -> putStrLn "Allocated a vote " >> sClose s >> tallyVotes raft (voteCount + 1 ) 
                           (RequestVoteReply term' False )  -> putStrLn "Rejected if stgmnt" >>
                                                               if term' > (currentTerm raft)
                                                               then putStrLn "I'm behind in term"  >> return (False, raft {currentTerm = term'
                                                                                                                          ,role = Follower
                                                                                                                          ,votedFor = Nothing })
                                                               else sClose s >> tallyVotes raft voteCount -- ^ try again. 
                         Right (MRequestVote reqV)  -> do
                           putStrLn $ "Received request for a vote from another candidate: " ++ show peer 
                           (msg, raft')   <- return $ handleRequestVote raft reqV peer
                           void $ NBS.sendTo s (encode msg) peer
                           sClose s -- ^ closing the socket here, so there is no issues next time. Why is this the issue. 
                           tallyVotes raft' voteCount
                         Right (MHeartbeat hb)  -> do
                           if leaderTerm hb >= currentTerm raft
                             then do
                             let raftFollS = raft { role = Follower }                               
                             return $ (False, raftFollS)
                             else
                             do
                               tallyVotes raft voteCount

testCC :: String  -> IO ()
testCC str = do
  raft  <-  initSystem str
  runAsCandidate (raft {role = Candidate})

runSystem :: RaftState  -> IO ()
runSystem raft = case role raft of
  Follower  -> runAsFollower raft
  (Leader _)  -> runAsLeader raft
  Candidate  -> runAsCandidate raft 


main :: IO ()
main = do
  args  <- getArgs  -- this will be a value : what node number is this
  raft  <- initSystem (head args)
  runSystem raft





{-

TODO

1. sendMessage does not retry if peer is down. ( need a way of detecting that.)
2. receiveMsgAsFollower  ->  need to implement  AppendEntrie Messages
3. In tallyVotes I need to take care of the fact that he may recieve a heartbeat message from another leader or a RequestForVote from another candidate. 

5. Debug: When both are candidates shit hits the fan. ( debugged )

7. In runAsFollower right now only timing out if i don't receive a message for a certain amount of time.
   need to time out if i during that amount of time, I haven't received a heartbeat or given a vote regardless of what messages i've received.


9. If candidate/follower receives heartbeat whose term is out of date, leader should quit and become a follower, update term to up to date value.

-}
      
{-

important notes
1. if teh followeres crash or run slowly, leader retries AppendEntriesRPC indefinitely ( even after it has responded to the client )
   until all followeres eventually store all log entries.
2. Next Steps : Leader State  -> Candidate State  -> AppendEntriesRPC  -> Adhere to Election and Commit Safety Rules 

-}
  





                   
