namespace Company.models
{

    public record ReplicaSet(
        string id,
        bool isInitialized,
        string databaseName,
        string tableName,
        //string PKColumnName,
        //string[] OtherColumns,
        long syncLocation
    );

}