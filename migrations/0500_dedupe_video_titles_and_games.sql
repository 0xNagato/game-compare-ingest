-- Dedupe video_game_titles and video_games on canonical keys
-- Idempotent: safe to re-run; if no duplicates exist, statements are no-ops.
BEGIN;

-- Ensure audit table exists for traceability
CREATE TABLE
    IF NOT EXISTS public.video_game_title_dedupe_audit (
        winner_id bigint NOT NULL REFERENCES public.video_game_titles (id) ON DELETE CASCADE,
        loser_id bigint NOT NULL,
        loser_video_game_id bigint,
        loser_title text,
        loser_normalized_title text,
        loser_created_at timestamptz,
        logged_at timestamptz NOT NULL DEFAULT now (),
        PRIMARY KEY (winner_id, loser_id)
    );

CREATE TABLE
    IF NOT EXISTS public.video_games_dedupe_audit (
        winner_id bigint NOT NULL REFERENCES public.video_games (id) ON DELETE CASCADE,
        loser_id bigint NOT NULL,
        loser_title_id bigint,
        loser_platform_id bigint,
        loser_edition text,
        loser_created_at timestamptz,
        logged_at timestamptz NOT NULL DEFAULT now (),
        PRIMARY KEY (winner_id, loser_id)
    );

-- 1) Dedupe video_game_titles on canonical normalized title
CREATE TEMP TABLE tmp_vgt_ranked AS
WITH
    canonical AS (
        SELECT
            id,
            video_game_id,
            COALESCE(NULLIF(normalized_title, ''), lower(title)) AS canonical_title,
            created_at
        FROM
            public.video_game_titles
    ),
    dupes AS (
        SELECT
            canonical_title
        FROM
            canonical
        WHERE
            canonical_title IS NOT NULL
        GROUP BY
            canonical_title
        HAVING
            COUNT(*)>1
    )
SELECT
    vgt.id,
    vgt.video_game_id,
    vgt.title,
    vgt.normalized_title,
    vgt.created_at,
    c.canonical_title,
    (
        EXISTS (
            SELECT
                1
            FROM
                public.sellables s
            WHERE
                s.software_title_id=vgt.id
        )
    ) AS has_sellables,
    (
        EXISTS (
            SELECT
                1
            FROM
                public.video_game_title_sources ts
            WHERE
                ts.video_game_title_id=vgt.id
        )
    ) AS has_sources,
    ROW_NUMBER() OVER (
        PARTITION BY
            c.canonical_title
        ORDER BY
            (
                EXISTS (
                    SELECT
                        1
                    FROM
                        public.sellables s
                    WHERE
                        s.software_title_id=vgt.id
                )
                OR EXISTS (
                    SELECT
                        1
                    FROM
                        public.video_game_title_sources ts
                    WHERE
                        ts.video_game_title_id=vgt.id
                )
            ) DESC,
            vgt.created_at ASC,
            vgt.id ASC
    ) AS rn,
    FIRST_VALUE (vgt.id) OVER (
        PARTITION BY
            c.canonical_title
        ORDER BY
            (
                EXISTS (
                    SELECT
                        1
                    FROM
                        public.sellables s
                    WHERE
                        s.software_title_id=vgt.id
                )
                OR EXISTS (
                    SELECT
                        1
                    FROM
                        public.video_game_title_sources ts
                    WHERE
                        ts.video_game_title_id=vgt.id
                )
            ) DESC,
            vgt.created_at ASC,
            vgt.id ASC
    ) AS winner_id
FROM
    public.video_game_titles vgt
    JOIN canonical c ON c.id=vgt.id
WHERE
    c.canonical_title IN (
        SELECT
            canonical_title
        FROM
            dupes
    );

-- update dependents for loser rows
UPDATE public.video_games vg
SET
    title_id=r.winner_id
FROM
    tmp_vgt_ranked r
WHERE
    r.rn>1
    AND vg.title_id=r.id;

UPDATE public.video_game_title_sources ts
SET
    video_game_title_id=r.winner_id
FROM
    tmp_vgt_ranked r
WHERE
    r.rn>1
    AND ts.video_game_title_id=r.id;

-- Reassign offers from loser sellables to an existing sellable for the winner, if present.
-- This prevents unique constraint violations when multiple sellables would point to the same winner.
-- First, identify loser/keep sellables and perform the reassignment in a single query
UPDATE public.offers o
SET
    sellable_id=keep.id
FROM
    public.sellables s
    JOIN tmp_vgt_ranked r ON s.software_title_id=r.id
    JOIN public.sellables keep ON keep.software_title_id=r.winner_id
WHERE
    r.rn>1
    AND o.sellable_id=s.id;

-- Delete any loser sellables that no longer have offers (they were merged above)
DELETE FROM public.sellables s USING tmp_vgt_ranked r
WHERE
    r.rn>1
    AND s.software_title_id=r.id
    AND NOT EXISTS (
        SELECT
            1
        FROM
            public.offers o
        WHERE
            o.sellable_id=s.id
    );

-- Finally, for remaining sellables that still reference loser titles (and no keep existed), update them to the winner_id
UPDATE public.sellables s
SET
    software_title_id=r.winner_id
FROM
    tmp_vgt_ranked r
WHERE
    r.rn>1
    AND s.software_title_id=r.id
    AND NOT EXISTS (
        SELECT
            1
        FROM
            public.sellables keep
        WHERE
            keep.software_title_id=r.winner_id
    );

INSERT INTO
    public.video_game_title_dedupe_audit (
        winner_id,
        loser_id,
        loser_video_game_id,
        loser_title,
        loser_normalized_title,
        loser_created_at
    )
SELECT
    winner_id,
    id,
    video_game_id,
    title,
    normalized_title,
    created_at
FROM
    tmp_vgt_ranked
WHERE
    rn>1 ON CONFLICT (winner_id, loser_id) DO NOTHING;

DELETE FROM public.video_game_titles USING tmp_vgt_ranked
WHERE
    public.video_game_titles.id=tmp_vgt_ranked.id
    AND tmp_vgt_ranked.rn>1;

-- 2) Dedupe video_games on (title_id, platform_id, edition)
-- Only run if game_media table exists (it's used in the ranking logic)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_media') THEN
    CREATE TEMP TABLE tmp_vg_ranked AS
    WITH
        canonical_games AS (
            SELECT
                id,
                title_id,
                platform_id,
                COALESCE(edition, '') AS edition_key,
                created_at
            FROM
                public.video_games
        ),
        dupe_games AS (
            SELECT
                title_id,
                platform_id,
                edition_key
            FROM
                canonical_games
            GROUP BY
                title_id,
                platform_id,
                edition_key
            HAVING
                COUNT(*)>1
        )
    SELECT
        vg.id,
        vg.title_id,
        vg.platform_id,
        vg.edition,
        vg.created_at,
        cg.edition_key,
        (
            EXISTS (
                SELECT
                    1
                FROM
                    public.game_media gm
                WHERE
                    gm.video_game_id=vg.id
                    AND gm.kind='image'
            )
        ) AS has_images,
        (
            EXISTS (
                SELECT
                    1
                FROM
                    public.game_media gm
                WHERE
                    gm.video_game_id=vg.id
                    AND gm.kind='video'
            )
        ) AS has_videos,
        (
            EXISTS (
                SELECT
                    1
                FROM
                    public.provider_media_links pml
                WHERE
                    pml.video_game_id=vg.id
            )
        ) AS has_media_links,
        (
            EXISTS (
                SELECT
                    1
                FROM
                    public.video_game_ratings_by_locale vrl
                WHERE
                    vrl.video_game_id=vg.id
            )
        ) AS has_ratings,
        ROW_NUMBER() OVER (
            PARTITION BY
                cg.title_id,
                cg.platform_id,
                cg.edition_key
            ORDER BY
                (
                    (
                        EXISTS (
                            SELECT
                                1
                            FROM
                                public.game_media gm
                            WHERE
                                gm.video_game_id=vg.id
                                AND gm.kind='image'
                        )
                    )
                    OR (
                        EXISTS (
                            SELECT
                                1
                            FROM
                                public.game_media gm
                            WHERE
                                gm.video_game_id=vg.id
                                AND gm.kind='video'
                        )
                    )
                    OR (
                        EXISTS (
                            SELECT
                                1
                            FROM
                                public.provider_media_links pml
                            WHERE
                                pml.video_game_id=vg.id
                        )
                    )
                    OR (
                        EXISTS (
                            SELECT
                                1
                            FROM
                                public.video_game_ratings_by_locale vrl
                            WHERE
                                vrl.video_game_id=vg.id
                        )
                    )
                ) DESC,
                vg.created_at ASC,
                vg.id ASC
        ) AS rn,
        FIRST_VALUE (vg.id) OVER (
            PARTITION BY
                cg.title_id,
                cg.platform_id,
                cg.edition_key
            ORDER BY
                (
                    (
                        EXISTS (
                            SELECT
                                1
                            FROM
                                public.game_media gm
                            WHERE
                                gm.video_game_id=vg.id
                                AND gm.kind='image'
                        )
                    )
                    OR (
                        EXISTS (
                            SELECT
                                1
                            FROM
                                public.game_media gm
                            WHERE
                                gm.video_game_id=vg.id
                                AND gm.kind='video'
                        )
                    )
                    OR (
                        EXISTS (
                            SELECT
                                1
                            FROM
                                public.provider_media_links pml
                            WHERE
                                pml.video_game_id=vg.id
                        )
                    )
                    OR (
                        EXISTS (
                            SELECT
                                1
                            FROM
                                public.video_game_ratings_by_locale vrl
                            WHERE
                                vrl.video_game_id=vg.id
                        )
                    )
                ) DESC,
                vg.created_at ASC,
                vg.id ASC
        ) AS winner_id
    FROM
        public.video_games vg
        JOIN canonical_games cg ON cg.id=vg.id
    WHERE
        (cg.title_id, cg.platform_id, cg.edition_key) IN (
            SELECT
                title_id,
                platform_id,
                edition_key
            FROM
                dupe_games
        );

    UPDATE public.game_media gm
    SET
        video_game_id=rg.winner_id
    FROM
        tmp_vg_ranked rg
    WHERE
        rg.rn>1
        AND gm.video_game_id=rg.id;

    UPDATE public.provider_media_links pml
    SET
        video_game_id=rg.winner_id
    FROM
        tmp_vg_ranked rg
    WHERE
        rg.rn>1
        AND pml.video_game_id=rg.id;

    UPDATE public.video_game_ratings_by_locale vrl
    SET
        video_game_id=rg.winner_id
    FROM
        tmp_vg_ranked rg
    WHERE
        rg.rn>1
        AND vrl.video_game_id=rg.id;

    INSERT INTO
        public.video_games_dedupe_audit (
            winner_id,
            loser_id,
            loser_title_id,
            loser_platform_id,
            loser_edition,
            loser_created_at
        )
    SELECT
        winner_id,
        id,
        title_id,
        platform_id,
        edition,
        created_at
    FROM
        tmp_vg_ranked
    WHERE
        rn>1 ON CONFLICT (winner_id, loser_id) DO NOTHING;

    DELETE FROM public.video_games USING tmp_vg_ranked
    WHERE
        public.video_games.id=tmp_vg_ranked.id
        AND tmp_vg_ranked.rn>1;
  END IF;
END $$;

COMMIT;