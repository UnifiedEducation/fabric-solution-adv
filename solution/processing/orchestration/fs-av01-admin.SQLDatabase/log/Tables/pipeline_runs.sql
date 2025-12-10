CREATE TABLE [log].[pipeline_runs] (
    [run_id]            BIGINT         IDENTITY (1, 1) NOT NULL,
    [pipeline_name]     VARCHAR (100)  NOT NULL,
    [started_at]        DATETIME2 (7)  DEFAULT (getdate()) NULL,
    [completed_at]      DATETIME2 (7)  NULL,
    [status]            VARCHAR (20)   NOT NULL,
    [records_processed] INT            NULL,
    [error_message]     NVARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([run_id] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_pipeline_runs_status]
    ON [log].[pipeline_runs]([status] ASC, [started_at] ASC);


GO

