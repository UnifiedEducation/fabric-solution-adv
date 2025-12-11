CREATE TABLE [metadata].[log_store] (
    [log_id]          INT            NOT NULL,
    [function_name]   VARCHAR (100)  NOT NULL,
    [description]     VARCHAR (1000) NULL,
    [expected_params] JSON           NULL,
    PRIMARY KEY CLUSTERED ([log_id] ASC)
);


GO

