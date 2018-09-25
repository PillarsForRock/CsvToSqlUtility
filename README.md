# CsvToSqlUtility
Utility to import all CSV files in a directory to a SQL Database and automatically creating the best field type for each field.

To run, add a app.ConnectionStrings.config file with following content and replace with the correct parameter values.
```xml
<?xml version="1.0" encoding="utf-8" ?>
<connectionStrings>
    <add name="DbContext" connectionString="Data Source=localhost;Initial Catalog=[databaseName]; User Id=[username]; password=[password];MultipleActiveResultSets=true" providerName="System.Data.SqlClient"/>
</connectionStrings>
```

Update the app.config file with the correct source folder and optional database table prefix.

The utility will then create a table in the target database for each csv file in the source folder.
