package org.foreverfzl.cloudcache.storage.instance;

import org.junit.Test;

/** Offline regression tests. No real S3 service or user WAL directory is accessed. */
public class IntegrityRegressionTest {
    @Test(timeout = 60000)
    public void futureWaitsForRemoteConfirmation() throws Exception {
        S3CloudCacheInstanceText.offlineFutureConfirmationTest();
    }

    @Test(timeout = 90000)
    public void failedUploadPreservesWalAndRestarts() throws Exception {
        S3CloudCacheInstanceText.offlineFailedUploadRecoveryTest();
    }

    @Test(timeout = 90000)
    public void concurrentMixedWritesSurviveBlockReuseAndFileRotation() throws Exception {
        S3CloudCacheInstanceText.offlineConcurrentIntegrityTest();
    }

    @Test(timeout = 60000)
    public void concurrentWriterLookupReturnsOneWriter() throws Exception {
        S3CloudCacheInstanceText.offlineWriterSingletonTest();
    }

    @Test(timeout = 60000)
    public void closingOneInstanceDoesNotDisableAnother() throws Exception {
        S3CloudCacheInstanceText.offlineInstanceIsolationTest();
    }

    @Test(timeout = 90000)
    public void cleanRestartDoesNotOverwriteConfirmedObjects() throws Exception {
        S3CloudCacheInstanceText.offlineRestartKeyUniquenessTest();
    }

    @Test(timeout = 90000)
    public void restartSkipsConfirmedBlockBeyondFailedGap() throws Exception {
        S3CloudCacheInstanceText.offlineNonContiguousConfirmationTest();
    }

    @Test(timeout = 90000)
    public void brokenBlockRecoveryWaitsForLateOriginalAppend() throws Exception {
        S3CloudCacheInstanceText.offlineRuntimeBrokenBlockRecoveryTest();
    }

    @Test(timeout = 90000)
    public void interruptedPoolWaitRetainsWalAndCompletesFailure() throws Exception {
        S3CloudCacheInstanceText.offlineInterruptedPoolWaitTest();
    }

    @Test(timeout = 90000)
    public void corruptWalDoesNotUploadValidPrefixOrDeleteOriginal() throws Exception {
        S3CloudCacheInstanceText.offlineCorruptWalRecoveryTest();
    }

    @Test(timeout = 90000)
    public void zeroWalReservationHoleDoesNotHideLaterRecord() throws Exception {
        S3CloudCacheInstanceText.offlineZeroHoleWalRecoveryTest();
    }

    @Test(timeout = 90000)
    public void corruptBucketMetadataRejectsWritesWithoutOverwritingWal() throws Exception {
        S3CloudCacheInstanceText.offlineCorruptBucketMetadataTest();
    }
}
