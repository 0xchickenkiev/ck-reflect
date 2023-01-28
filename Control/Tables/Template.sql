CREATE TABLE [Control].[Template] (
    [TemplateId]     INT           IDENTITY (1, 1) NOT NULL,
    [Task]           VARCHAR (30)  NOT NULL,
    [Action]         VARCHAR (20)  NOT NULL,
    [SourceForm]     CHAR (3)      NOT NULL,
    [TargetForm]     CHAR (3)      NOT NULL,
    [SQL]            VARCHAR (MAX) NOT NULL,
    [ActionPriority] INT           NOT NULL,
    [Optional]       CHAR (1)      NOT NULL,
    [Enabled]        CHAR (1)      DEFAULT ('Y') NOT NULL,
    [CreatedDate]    DATETIME      DEFAULT (sysdatetime()) NOT NULL,
    [ModifiedDate]   DATETIME      DEFAULT (sysdatetime()) NOT NULL
);


GO

CREATE TRIGGER [Control].[Template_AI] 
ON [Control].[Template]
FOR INSERT AS
BEGIN
	UPDATE	q SET Enabled = 'N' 
	FROM	Control.Template q 
			INNER JOIN inserted i ON (i.Task = q.Task AND i.Action = q.Action AND i.TemplateId <> q.TemplateId AND i.SourceForm = q.SourceForm AND i.TargetForm = q.TargetForm)
	WHERE	q.Enabled = 'Y'
	UPDATE  Control.Template
			SET CreatedDate = (	SELECT	MIN(CreatedDate)
									FROM	Control.Template q1
									WHERE	q1.Task = Template.Task AND q1.Action = Template.Action AND q1.SourceForm = Template.SourceForm AND q1.TargetForm = Template.TargetForm)
	WHERE	TemplateId IN (SELECT TemplateId FROM inserted)
END