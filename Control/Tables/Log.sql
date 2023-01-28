CREATE TABLE [Control].[Log](
	[BatchDate] [datetime2](7) NOT NULL,
	[Batch] [varchar](50) NOT NULL,
	[Task] [varchar](50) NOT NULL,
	[Step] [varchar](50) NOT NULL,
	[ServerName] [varchar](128) NOT NULL,
	[DatabaseName] [varchar](128) NOT NULL,
	[SchemaName] [varchar](128) NOT NULL,
	[ObjectName] [varchar](128) NOT NULL,
	[Outcome] [varchar](10) NOT NULL,
	[StartTime] [datetime2](7) NULL,
	[EndTime] [datetime2](7) NULL,
	[Error] [int] NULL,
	[ErrorMessage] [varchar](2048) NULL,
	[Query] [varchar](max) NULL,
	[FromDate] [datetime2](7) NULL,
	[ToDate] [datetime2](7) NULL,
	[Additional1] [varchar](2048) NULL,
	[Additional2] [varchar](2048) NULL,
	[Additional3] [varchar](2048) NULL,
	[Additional4] [varchar](2048) NULL,
	[InsertCount] [int] NULL,
	[UpdateCount] [int] NULL,
	[DeleteCount] [int] NULL,
	[UserId] [int] NULL,
	[UserName] [varchar](256) NULL
)
GO

CREATE CLUSTERED INDEX [CI_Log] ON [Control].[Log]
(
	[BatchDate] ASC,
	[Batch] ASC,
	[Task] ASC,
	[Step] ASC,
	[ServerName] ASC,
	[DatabaseName] ASC,
	[SchemaName] ASC,
	[ObjectName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
GO


