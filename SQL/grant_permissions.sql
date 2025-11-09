USE Patient_Records;
GO

-- If user exists, drop and recreate it to ensure clean mapping
DROP USER IF EXISTS databricks_loader;
GO

CREATE USER databricks_loader FOR LOGIN databricks_loader;
GO

ALTER ROLE db_owner ADD MEMBER databricks_loader;
GO


SELECT dp.name AS UserName, dp.type_desc AS Type, drp.name AS RoleName
FROM sys.database_principals dp
LEFT JOIN sys.database_role_members drm ON dp.principal_id = drm.member_principal_id
LEFT JOIN sys.database_principals drp ON drm.role_principal_id = drp.principal_id
WHERE dp.name = 'databricks_loader';
