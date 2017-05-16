
//using Microsoft.WindowsAzure.Storage.Core.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
namespace Microsoft.WindowsAzure.Storage.Blob
{
    public class ParallelDownload
    {
        public readonly ParallelDownloadSettings ParallelDownloadSettings;

        public Task Task { get; private set; }

        //private int parallelIOCount;

        //private long chunkSize;

        private long? offset;

        private long? length;

        //private string path;

        //private FileMode fileMode;

        List<MemoryMappedViewStream> viewStreams;

        List<Task> downloadTaskList;
        private readonly Object _lock;
        enum State { Running, Paused }
        private State state = State.Running;
        private TaskCompletionSource<bool> ResumeTask = new TaskCompletionSource<bool>();
        private TaskCompletionSource<CancellationToken> CancellationTask;
        private List<Task> continuationTasks = new List<Task>();

        //public void Pause()
        //{
        //    Monitor.Enter(_lock);
        //    if (state == State.Running)
        //    {
        //        Volatile.Write(ref ResumeTask, new TaskCompletionSource<bool>());
        //        state = State.Paused;
        //    }
        //    Monitor.Exit(_lock);
        //}

        public void Resume()
        {
            Monitor.Enter(_lock);
            if (state == State.Paused)
            {
                ResumeTask.SetResult(true);
                state = State.Running;
            }
            Monitor.Exit(_lock);
        }

        private ParallelDownload(ParallelDownloadSettings parallelDownloadSettings)
        {
            this.ParallelDownloadSettings = parallelDownloadSettings;
            this.viewStreams = new List<MemoryMappedViewStream>();
            this.ResumeTask.SetResult(true);
            this.offset = parallelDownloadSettings.Offset;
            this.length = parallelDownloadSettings.Length;
        }

        public static ParallelDownload Start(ParallelDownloadSettings parallelDownloadSettings)
        {
            return Start(parallelDownloadSettings, null, null, null, CancellationToken.None);
        }

        public static ParallelDownload Start(ParallelDownloadSettings parallelDownloadSettings, AccessCondition accessConditions, OperationContext operationContext, BlobRequestOptions options, CancellationToken cancellationToken)
        {
            ParallelDownload parallelDownload = new ParallelDownload(parallelDownloadSettings);
            parallelDownload.Task = StartAsync(parallelDownload, accessConditions, operationContext, options, cancellationToken);

            return parallelDownload;
        }

        private static async Task StartAsync(ParallelDownload parallelDownload, AccessCondition accessConditions, OperationContext operationContext, BlobRequestOptions options, CancellationToken cancellationToken)
        {
            ParallelDownloadSettings downloadSettings = parallelDownload.ParallelDownloadSettings;
            parallelDownload.CancellationTask = new TaskCompletionSource<CancellationToken>(cancellationToken);
            parallelDownload.continuationTasks = new List<Task>();
            //this.continuationTasks.Add(this.CancellationTask);
            //this.continuationTasks.Add(this.ResumeTask);
            long timeStamp = Stopwatch.GetTimestamp();
            CloudBlob blob = downloadSettings.Blob;
            if (!parallelDownload.length.HasValue)
            {
                if (blob.Properties.Length == -1)
                {
                    await blob.FetchAttributesAsync(accessConditions, options, operationContext);
                }

                parallelDownload.length = blob.Properties.Length;
            }

            if (!downloadSettings.Offset.HasValue)
            {
                parallelDownload.offset = 0;
            }

            parallelDownload.downloadTaskList = new List<Task>();
            int totalIOReadCalls = (int)Math.Ceiling((double)parallelDownload.length.Value / (double)downloadSettings.MaxRangeSize);
            try
            {
                using (MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(downloadSettings.FilePath, downloadSettings.FileMode, null, parallelDownload.length.Value))
                {
                    for (int i = 0; i < totalIOReadCalls; i++)
                    {
                        if (parallelDownload.downloadTaskList.Count >= downloadSettings.ParallelIOCount)
                        {
                            try
                            {

                                // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                                Task downloadChunk = await Task.WhenAny(parallelDownload.downloadTaskList).ConfigureAwait(false);
                                //downloadChunk
                                parallelDownload.downloadTaskList.Remove(downloadChunk);
                            }
                            catch (StorageException)
                            {
                                throw;
                            }
                        }

                        await Volatile.Read(ref parallelDownload.ResumeTask).Task;
                        long streamBeginIndex = parallelDownload.offset.Value + (i * downloadSettings.MaxRangeSize);
                        long streamReadSize = downloadSettings.MaxRangeSize;
                        // last range may be smaller than the chunk size
                        if (i == totalIOReadCalls - 1)
                        {
                            streamReadSize = parallelDownload.length.Value - (parallelDownload.offset.Value + (i * streamReadSize));
                        }

                        MemoryMappedViewStream viewStream = mmf.CreateViewStream(streamBeginIndex, streamReadSize);
                        parallelDownload.viewStreams.Add(viewStream);
                        Task downloadTask = downloadSettings.Blob.DownloadRangeToStreamAsync(viewStream, streamBeginIndex, streamReadSize, accessConditions, options, operationContext, cancellationToken);
                        parallelDownload.downloadTaskList.Add(downloadTask);
                    }

                    await Task.WhenAll(parallelDownload.downloadTaskList).ConfigureAwait(false);
                }
            }
            finally
            {
                foreach (var viewStream in parallelDownload.viewStreams)
                {
                    viewStream.Dispose();
                }
            }
        }
    }
}
