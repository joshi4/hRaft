module Hraft (initSystem) where

import Data.Serialize
import System.Random hiding (split )
import qualified Data.Map.Strict as Map
import Election
import Types
import Utility

electionTimerLL = 150000 -- ^ Lower Limit for election Time out 
electionTimerUL = 500000 -- ^ Upper Limit for election Time out


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
  ,getlog = [LogBlock {logterm = 0 , instruction = encode ""}]
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
  rs'  <-  readRaftFile rs -- reads from file if possible
  putStrLn $ "Initial Log : " ++ show (getlog rs')
  return $ rs' {electionTimeOut = electionTimer} -- new random election timer
   
{-   INITIALIZATION CODE ENDS HERE    -}


testBS :: IO ()
testBS = do
  raft  <- initSystem "1"
  writeRaftToFile raft
  raft'  <- readRaftFile raft
  putStrLn $ show raft' 
  
