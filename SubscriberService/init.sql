CREATE TABLE IF NOT EXISTS "Subscribers" (
                                             "Id" UUID PRIMARY KEY,
                                             "Email" TEXT NOT NULL,
                                             "SubscribedAt" TIMESTAMPTZ NOT NULL,
                                             "UnsubscribedAt" TIMESTAMPTZ NULL,
                                             "Region" TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS "IX_Subscribers_Email" ON "Subscribers" ("Email");
CREATE INDEX IF NOT EXISTS "IX_Subscribers_Region" ON "Subscribers" ("Region");