CREATE TABLE [Control].[Version]
(
	[Id] INT Identity (1,1) NOT NULL PRIMARY KEY
	, VersionID varchar(100) NOT NULL
	, DeployedOn datetime2 NOT NULL
	, DeployedBy varchar(100) NOT NULL
)
