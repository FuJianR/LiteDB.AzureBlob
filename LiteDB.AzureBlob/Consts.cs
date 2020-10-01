using Microsoft.Azure.Storage.Blob;

namespace LiteDB.AzureBlob
{
    static class Consts
    {
        internal static int PageSize = 1024 * 8;  // Default PageSize of LiteDB 5
        internal static int Pages = 8;
        internal static string DefaultContainerName = "litedbs";
        internal static long DefaultStreamSize = 1024 * 1024 * 1024; //1G
        internal static PremiumPageBlobTier DefaultBlobTier = PremiumPageBlobTier.P4;
        internal const string MetadataLengthKey = "STREAM_LENGTH";
        internal const int NumberOfLocks = 111;  // should be more than enough
    }
}
