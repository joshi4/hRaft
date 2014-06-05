
module Utility where

import Network.Socket  hiding (send, sendTo, recv, recvFrom )
import qualified Data.Map.Strict as Map
import qualified Network.Socket.ByteString as NBS 
import qualified Data.ByteString as BS
import Control.Exception
import Data.Serialize
import Data.List
import Control.Monad
import System.IO 
import System.Directory (doesFileExist)
import Types

-- Simple version , not retrying if failed: i:e when peer is down/shutoff.   
sendMessage :: Message  -> SockAddr -> SockAddr  -> IO ()
sendMessage msg myaddr peer = bracket (socket AF_INET Datagram 0) sClose
                       (\s  -> do                           
                           bind s myaddr -- ^ don't bind it to peer but bind it to your own address.
                           --putStrLn "Finished binding"
                           void $ NBS.sendTo s (encode msg) peer  -- ^ May need some mechanism to retry if it has failed.
                           -- putStrLn "end of sendMessage"
                       )



extractSockAddr :: [(String, SockAddr)] -> [SockAddr]
extractSockAddr xs = [sa | (_,sa) <- xs ]

getMySockAddr :: RaftState  -> SockAddr
getMySockAddr raft = let myid = myNode raft
                         config = participantsMap raft
                     in
                      case Map.lookup myid config of
                        Nothing  -> undefined -- ^ Error should not happen 
                        (Just x ) -> x 

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



-- Being hanuman. Instead of stroing the relevant fields
-- am storing the whole state. Deferring cherry-picking to
-- read stage        
writeRaftToFile :: RaftState  -> IO ()
writeRaftToFile raft = do
  let path = raftFilePath (myNode raft)
  BS.writeFile path (encode raft)



-- Read the binary file and decode it.
-- only update the required fields with stored content
-- rest all is volatile state.
-- if file does not exist then it will take
-- the initial supplied value.   
readRaftFile :: RaftState  -> IO RaftState
readRaftFile raft = do
  let path = raftFilePath $ myNode raft
  exists  <- doesFileExist path
  if exists then
    do
      bys <- BS.readFile path
      case decode bys of
        Left _  -> putStrLn "Error! in decoding from file " >> return raft
        Right r  -> return $ raft { currentTerm = currentTerm r ,
                                    getlog = getlog r ,
                                    votedFor = votedFor r 
                                  }
    else
    do
      return raft 

-- Given the node number , generates the file path. 
raftFilePath :: String  -> FilePath
raftFilePath node = "/Users/shantanu/" ++ "RaftNode" ++ node ++ ".raft"
