CREATE TABLE [log].[validation_results] (
    [result_id]           BIGINT        IDENTITY (1, 1) NOT NULL,
    [run_id]              BIGINT        NULL,
    [validation_instr_id] INT           NULL,
    [expectation_type]    VARCHAR (100) NULL,
    [column_name]         VARCHAR (100) NULL,
    [passed]              BIT           NOT NULL,
    [observed_value]      JSON          NULL,
    [executed_at]         DATETIME2 (7) DEFAULT (getdate()) NULL,
    [lakehouse_name]      VARCHAR (100) NULL,
    [schema_name]         VARCHAR (50)  NULL,
    [table_name]          VARCHAR (100) NULL,
    PRIMARY KEY CLUSTERED ([result_id] ASC),
    CONSTRAINT [FK_validation_run] FOREIGN KEY ([run_id]) REFERENCES [log].[pipeline_runs] ([run_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_validation_results_run]
    ON [log].[validation_results]([run_id] ASC);


GO

