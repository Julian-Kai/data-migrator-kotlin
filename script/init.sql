CREATE TABLE "public"."temp_anonymous" (
    "chat_id" varchar NOT NULL,
    "alias_id" int8 NOT NULL,
    "user_id" varchar NOT NULL,
    "nickname" varchar,
    "avatar" varchar,
    PRIMARY KEY ("chat_id","user_id")
);