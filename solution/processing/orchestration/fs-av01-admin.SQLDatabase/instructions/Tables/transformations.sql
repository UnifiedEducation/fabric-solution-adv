CREATE TABLE [instructions].[transformations] (
    [transform_instr_id] INT           NOT NULL,
    [source_table]       VARCHAR (200) NOT NULL,
    [source_layer]       VARCHAR (20)  NOT NULL,
    [dest_table]         VARCHAR (200) NOT NULL,
    [dest_layer]         VARCHAR (20)  NOT NULL,
    [transform_pipeline] JSON          NOT NULL,
    [transform_params]   JSON          NULL,
    [merge_condition]    VARCHAR (500) NULL,
    [merge_type]         VARCHAR (20)  DEFAULT ('update_all') NULL,
    [merge_columns]      JSON          NULL,
    [is_active]          BIT           DEFAULT ((1)) NULL,
    [log_function_id]    INT           NULL,
    [created_date]       DATETIME2 (7) DEFAULT (getdate()) NULL,
    [modified_date]      DATETIME2 (7) DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([transform_instr_id] ASC),
    CONSTRAINT [FK_transform_log] FOREIGN KEY ([log_function_id]) REFERENCES [metadata].[log_store] ([log_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_transformations_active]
    ON [instructions].[transformations]([is_active] ASC);


GO

