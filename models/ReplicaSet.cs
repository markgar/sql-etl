namespace Company.models
{

    public record ReplicaSet(
        string id,
        bool isInitialized,
        string databaseName,
        string tableName,
        long syncLocation
    );

}