CREATE TABLE IF NOT EXISTS "Articles" (
                                          "Id" uuid NOT NULL,
                                          "Title" text NOT NULL,
                                          "Content" text NOT NULL,
                                          "PublishedAt" timestamp with time zone NOT NULL,
                                          "ShardKey" text NOT NULL,
                                          CONSTRAINT "PK_Articles" PRIMARY KEY ("Id")
    );

CREATE INDEX IF NOT EXISTS "IX_Articles_ShardKey" ON "Articles" ("ShardKey");
CREATE INDEX IF NOT EXISTS "IX_Articles_PublishedAt" ON "Articles" ("PublishedAt" DESC);