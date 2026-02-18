CREATE TABLE [instructions].[validations] (
    [validation_instr_id] INT           NOT NULL,
    [target_table]        VARCHAR (200) NOT NULL,
    [target_layer]        VARCHAR (20)  NOT NULL,
    [expectation_id]      INT           NOT NULL,
    [column_name]         VARCHAR (100) NULL,
    [validation_params]   JSON          NULL,
    [severity]            VARCHAR (20)  DEFAULT ('error') NULL,
    [is_active]           BIT           DEFAULT ((1)) NULL,
    [log_function_id]     INT           NULL,
    [pipeline_name]       VARCHAR (100) NULL,
    [notebook_name]       VARCHAR (100) NULL,
    [created_date]        DATETIME2 (7) DEFAULT (getdate()) NULL,
    [modified_date]       DATETIME2 (7) DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([validation_instr_id] ASC),
    CONSTRAINT [FK_validation_expectation] FOREIGN KEY ([expectation_id]) REFERENCES [metadata].[expectation_store] ([expectation_id]),
    CONSTRAINT [FK_validation_log] FOREIGN KEY ([log_function_id]) REFERENCES [metadata].[log_store] ([log_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_validations_table]
    ON [instructions].[validations]([target_table] ASC, [is_active] ASC);


GO

