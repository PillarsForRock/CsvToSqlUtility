# CsvToSqlUtility
Utility to import CSV files to SQL automatically creating best field type

To run, add a app.ConnectionStrings.config file with following content:
```xml
<?xml version="1.0" encoding="utf-8" ?>
<connectionStrings>
    <add name="DbContext" connectionString="Data Source=localhost;Initial Catalog=[dbTablename]; User Id=[dbUsername]; password=[dbPassword];MultipleActiveResultSets=true" providerName="System.Data.SqlClient"/>
</connectionStrings>
```