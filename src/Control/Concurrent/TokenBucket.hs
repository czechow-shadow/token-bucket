{-# LANGUAGE CPP #-}

-- | This module provides rate-limiting facilities built on top of the lazy bucket algorithm heavily inspired by
-- <http://ksdlck.com/post/17418037348/rate-limiting-at-webscale-lazy-leaky-buckets "Rate Limiting at Webscale: Lazy Leaky Buckets">
--
-- See also Wikipedia's <http://en.wikipedia.org/wiki/Token_bucket Token Bucket> article for general information about token bucket algorithms and their properties.
module Control.Concurrent.TokenBucket
    ( -- * The 'TokenBucket' type
      TokenBucket
    , newTokenBucket
    , newTokenBucketWithTimeFun
    , getTBData

      -- * Operations on 'TokenBucket'
      --
      -- | The following operations take two parameters, a burst-size and an average token rate.
      --
      -- === Average token rate
      --
      -- The average rate is expressed as inverse rate in terms of
      -- microseconds-per-token (i.e. one token every
      -- @n@ microseconds). This representation exposes the time
      -- granularity of the underlying implementation using integer
      -- arithmetic.
      --
      -- So in order to convert a token-rate @r@ expressed in
      -- tokens-per-second (i.e. @Hertz@) to microseconds-per-token the
      -- simple function below can be used:
      --
      -- @
      -- toInvRate :: Double -> Word64
      -- toInvRate r = round (1e6 / r)
      -- @
      --
      -- An inverse-rate @0@ denotes an infinite average rate, which
      -- will let token allocation always succeed (regardless of the
      -- burst-size parameter).
      --
      -- === Burst size
      --
      -- The burst-size parameter denotes the depth of the token
      -- bucket, and allows for temporarily exceeding the average
      -- token rate. The burst-size parameter should be at least as
      -- large as the maximum amount of tokens that need to be
      -- allocated at once, since an allocation-size smaller than the
      -- current burst-size will always fail unless an infinite token
      -- rate is used.

    , tokenBucketTryAlloc
    , tokenBucketTryAlloc1
    , tokenBucketWait
    ) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.IORef
#if !defined(USE_CBITS)
import Data.Time.Clock.POSIX (getPOSIXTime)
#endif
import Data.Word (Word64)

-- | Abstract type containing the token bucket state and a function to
-- query for current time.
data TokenBucket = TB !(IORef TBData) !(IO PosixTimeUsecs)

data TBData = TBData !Word64 !PosixTimeUsecs
              deriving Show

type PosixTimeUsecs = Word64

getTBData :: TokenBucket -> IO TBData
getTBData (TB lbd _) = readIORef lbd

#if defined(USE_CBITS)
foreign import ccall unsafe "hs_token_bucket_get_posix_time_usecs"
    getPosixTimeUsecs :: IO PosixTimeUsecs
#else
getPosixTimeUsecs :: IO PosixTimeUsecs
getPosixTimeUsecs = fmap (floor . (*1e6)) getPOSIXTime
#endif

-- | Create new 'TokenBucket' instance
newTokenBucket :: IO TokenBucket
newTokenBucket = newTokenBucketWithTimeFun getPosixTimeUsecs


-- | Create new 'TokenBucket' instance with custom time function.
-- Used primarily for testing.
newTokenBucketWithTimeFun :: IO PosixTimeUsecs -> IO TokenBucket
newTokenBucketWithTimeFun timeFun = do
    now <- timeFun
    lbd <- newIORef $! TBData 0 now
    evaluate (TB lbd timeFun)

-- | Attempt to allocate a given amount of tokens from the 'TokenBucket'
--
-- This operation either succeeds in allocating the requested amount
-- of tokens (and returns 'True'), or else, if allocation fails the
-- 'TokenBucket' remains in its previous allocation state.
tokenBucketTryAlloc :: TokenBucket
                    -> Word64  -- ^ burst-size (tokens)
                    -> Word64  -- ^ avg. inverse rate (usec/token)
                    -> Word64  -- ^ amount of tokens to allocate
                    -> IO Bool -- ^ 'True' if allocation succeeded
tokenBucketTryAlloc _ _  0 _ = return True -- infinitive rate, no-op
tokenBucketTryAlloc _ burst _ alloc | alloc > burst = return False
tokenBucketTryAlloc (TB lbref timeFun) burst invRate alloc = do
    now <- timeFun
    atomicModifyIORef' lbref (go now)
  where
    go now (TBData lvl ts)
      | lvl'' > burst = (TBData lvl'  ts', False)
      | otherwise     = (TBData lvl'' ts', True)
      where
        lvl' = lvl ∸ dl
        (dl,dtRem) = dt `quotRem` invRate
        dt   = now ∸ ts
        ts'  = now ∸ dtRem

        lvl'' = lvl' ∔ alloc

-- | Try to allocate a single token from the token bucket.
--
-- Returns 0 if successful (i.e. a token was successfully allocated from
-- the token bucket).
--
-- On failure, i.e. if token bucket budget was exhausted, the minimum
-- non-zero amount of microseconds to wait till allocation /may/
-- succeed is returned.
--
-- This function does not block. See 'tokenBucketWait' for wrapper
-- around this function which blocks until a token could be allocated.
tokenBucketTryAlloc1 :: TokenBucket
                     -> Word64     -- ^ burst-size (tokens)
                     -> Word64     -- ^ avg. inverse rate (usec/token)
                     -> IO Word64  -- ^ retry-time (usecs)
tokenBucketTryAlloc1 _ _ 0 = return 0 -- infinite rate, no-op
tokenBucketTryAlloc1 (TB lbref timeFun) burst invRate = do
    now <- timeFun
    atomicModifyIORef' lbref (go now)
  where
    go now (TBData lvl ts)
      | lvl'' > burst = (TBData lvl'  ts', invRate-dtRem)
      | otherwise     = (TBData lvl'' ts', 0)
      where
        lvl' = lvl ∸ dl
        (dl,dtRem) = dt `quotRem` invRate
        dt   = now ∸ ts
        ts'  = now ∸ dtRem
        lvl'' = lvl' ∔ 1

-- | Blocking wrapper around 'tokenBucketTryAlloc1'. Uses 'threadDelay' when blocking.
--
-- This is effectively implemented as
--
-- @
-- 'tokenBucketWait' tb burst invRate = do
--   delay <- 'tokenBucketTryAlloc1' tb burst invRate
--   unless (delay == 0) $ do
--     threadDelay (fromIntegral delay)
--     'tokenBucketWait' tb burst invRate
-- @
tokenBucketWait :: TokenBucket
                -> Word64  -- ^ burst-size (tokens)
                -> Word64  -- ^ avg. inverse rate (usec/token)
                -> IO ()
tokenBucketWait tb burst invRate = do
    delay <- tokenBucketTryAlloc1 tb burst invRate
    unless (delay == 0) $ do
        threadDelay (fromIntegral delay)
        tokenBucketWait tb burst invRate

-- saturated arithmetic helpers
(∸), (∔) :: Word64 -> Word64 -> Word64
x ∸ y = if x>y then x-y else 0
{-# INLINE (∸) #-}
x ∔ y = let s=x+y in if x <= s then s else maxBound
{-# INLINE (∔) #-}
