using System.Threading.Tasks;

namespace LiteDB.AzureBlob
{
    internal static class TaskExtensions
    {
        internal static T SyncResult<T>(this Task<T> task)
            => task != null ? task.ConfigureAwait(false).GetAwaiter().GetResult() : default(T);

        internal static void SyncWait(this Task task)
            => task?.ConfigureAwait(false).GetAwaiter().GetResult();
    }
}
