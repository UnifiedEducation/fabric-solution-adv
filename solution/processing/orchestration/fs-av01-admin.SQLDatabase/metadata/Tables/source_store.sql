CREATE TABLE [metadata].[source_store] (
    [source_id]     INT            NOT NULL,
    [source_name]   VARCHAR (100)  NOT NULL,
    [source_type]   VARCHAR (50)   NOT NULL,
    [auth_method]   VARCHAR (50)   NULL,
    [key_vault_url] VARCHAR (500)  NULL,
    [secret_name]   VARCHAR (100)  NULL,
    [base_url]          VARCHAR (500)  NULL,
    [handler_function]  VARCHAR (100)  NULL,
    [description]       VARCHAR (1000) NULL,
    [created_date]  DATETIME2 (7)  DEFAULT (getdate()) NULL,
    [modified_date] DATETIME2 (7)  DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([source_id] ASC)
);


GO

