module Main where

import System.Environment (getArgs)
import Hraft
import Election

main :: IO ()
main = do
  args  <- getArgs  -- this will be a value : what node number is this
  raft  <- initSystem (head args)
  runSystem raft
  --runAsLeader $ changeRoleToLeader raft 
