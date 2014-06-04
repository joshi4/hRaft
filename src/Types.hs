{-# LANGUAGE DeriveGeneric #-}
module Types where 

import Network.Socket  hiding (send, sendTo, recv, recvFrom )
import qualified Data.Map.Strict as Map
import qualified Network.Socket.ByteString as NBS 
import qualified Data.ByteString as BS
import GHC.Generics
import Data.Serialize
-- import qualified Network.Socket as NBS hiding (send, sendTo, recv, recvFrom )




type Term = Int
type Host = String
type PortNum = String 


type Log = [LogBlock]

data LogBlock = LogBlock {
  logterm :: Term, 
  instruction :: BS.ByteString 
  } deriving ( Generic) 

instance Show LogBlock where
  show a = case decode $ instruction a of
    Left err  -> "error!"
    Right s  -> s 




data Message = MRequestVote RequestVote
             | MRequestVoteReply RequestVoteReply
             | MAppendEntries AppendEntries
             | MHeartbeat AppendEntries
             | MAppendEntriesReply AppendEntriesReply
             | Empty 
               deriving (Show, Generic)


data AppendEntries = AppendEntries {
  leaderTerm :: Term,
  leaderId :: SockAddr, -- ^ so follower can redirect the clients
  prevLogIndex :: Int,
  prevLogTerm :: Term,
  entries :: Log -- ^ EMPTY for heartbeat
  } deriving (Show, Generic )


data AppendEntriesReply = AppendEntriesReply {
  appTerm :: Term,
  success :: Bool
  } deriving (Show, Generic)

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
data Role = Follower
          | Leader {getLeaderState :: LeaderState }
          | Candidate
          deriving (Show, Generic, Eq )


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
instance Serialize AppendEntriesReply
instance Serialize LeaderState


--- END OF IMPORTS AND DATA DECLARATIONS ---- 
