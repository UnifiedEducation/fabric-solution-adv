CREATE TABLE [instructions].[ingestion] (
    [ingestion_id]    INT           NOT NULL,
    [source_id]       INT           NOT NULL,
    [endpoint_path]   VARCHAR (500) NULL,
    [landing_path]    VARCHAR (500) NOT NULL,
    [request_params]  JSON          NULL,
    [is_active]       BIT           DEFAULT ((1)) NULL,
    [log_function_id] INT           NULL,
    [pipeline_name]   VARCHAR (100) NULL,
    [notebook_name]   VARCHAR (100) NULL,
    [created_date]    DATETIME2 (7) DEFAULT (getdate()) NULL,
    [modified_date]   DATETIME2 (7) DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([ingestion_id] ASC),
    CONSTRAINT [FK_ingestion_log] FOREIGN KEY ([log_function_id]) REFERENCES [metadata].[log_store] ([log_id]),
    CONSTRAINT [FK_ingestion_source] FOREIGN KEY ([source_id]) REFERENCES [metadata].[source_store] ([source_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_ingestion_source]
    ON [instructions].[ingestion]([source_id] ASC, [is_active] ASC);


GO

