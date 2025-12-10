CREATE TABLE [instructions].[loading] (
    [loading_instr_id] INT           NOT NULL,
    [loading_id]       INT           NOT NULL,
    [source_path]      VARCHAR (500) NOT NULL,
    [source_layer]     VARCHAR (20)  DEFAULT ('raw') NOT NULL,
    [target_table]     VARCHAR (200) NOT NULL,
    [target_layer]     VARCHAR (20)  DEFAULT ('bronze') NOT NULL,
    [key_columns]      JSON          NOT NULL,
    [load_params]      JSON          NULL,
    [merge_condition]  VARCHAR (500) NULL,
    [merge_type]       VARCHAR (20)  DEFAULT ('update_all') NULL,
    [merge_columns]    JSON          NULL,
    [is_active]        BIT           DEFAULT ((1)) NULL,
    [log_function_id]  INT           NULL,
    [created_date]     DATETIME2 (7) DEFAULT (getdate()) NULL,
    [modified_date]    DATETIME2 (7) DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([loading_instr_id] ASC),
    CONSTRAINT [FK_loading_log] FOREIGN KEY ([log_function_id]) REFERENCES [metadata].[log_store] ([log_id]),
    CONSTRAINT [FK_loading_store] FOREIGN KEY ([loading_id]) REFERENCES [metadata].[loading_store] ([loading_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_loading_active]
    ON [instructions].[loading]([is_active] ASC);


GO

