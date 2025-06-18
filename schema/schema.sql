CREATE TABLE public.users (id uuid NOT NULL DEFAULT gen_random_uuid(),
    email text NOT NULL,
    password_hash text NOT NULL,
    full_name text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE (email),
    id IS NOT NULL,
    email IS NOT NULL,
    password_hash IS NOT NULL
);

