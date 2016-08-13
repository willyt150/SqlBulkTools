namespace SqlBulkTools
{
    internal static class Constants
    {
        internal const string DefaultSchemaName = "dbo";
        internal const string InternalId = "SqlBulkTools_InternalId";
        internal const string TempTableName = "#TmpTable";
        internal const string TempOutputTableName = "#TmpOutput";
        internal const string SourceAlias = "Source";
        internal const string TargetAlias = "Target";
        
    }

    internal static class IndexOperation
    {
        internal const string Rebuild = "REBUILD";
        internal const string Disable = "DISABLE";
    }



#pragma warning disable 1591
    public enum ColumnDirection
    {        
        Input, InputOutput       
    }
    #pragma warning restore 1591

    internal enum OperationType
    {
        Insert, InsertOrUpdate, Update, Delete
    }
}
