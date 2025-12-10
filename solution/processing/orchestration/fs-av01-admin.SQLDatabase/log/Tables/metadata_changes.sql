CREATE TABLE [log].[metadata_changes] (
    [change_id]   BIGINT        IDENTITY (1, 1) NOT NULL,
    [table_name]  VARCHAR (100) NOT NULL,
    [record_id]   INT           NOT NULL,
    [change_type] VARCHAR (20)  NOT NULL,
    [changed_by]  VARCHAR (100) DEFAULT (suser_sname()) NULL,
    [changed_at]  DATETIME2 (7) DEFAULT (getdate()) NULL,
    [old_values]  JSON          NULL,
    [new_values]  JSON          NULL,
    PRIMARY KEY CLUSTERED ([change_id] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_metadata_changes_table]
    ON [log].[metadata_changes]([table_name] ASC, [changed_at] ASC);


GO

