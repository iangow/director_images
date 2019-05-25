library(dplyr, warn.conflicts = FALSE)
library(RPostgreSQL)

pg <- dbConnect(PostgreSQL())

rs <- dbGetQuery(pg, "SET search_path TO director_photo, public")
equilar_schema <- "executive_gsb"

executive <- tbl(pg, sql(paste0("SELECT * FROM ", equilar_schema, ".executive ")))
proxy_board_director <-
    tbl(pg, sql(paste0("SELECT * FROM ", equilar_schema, ".proxy_board_director")))

rs <- dbGetQuery(pg, "DROP TABLE IF EXISTS equilar_photos_old")

equilar_photos_old <-
    proxy_board_director %>%
    select(executive_id) %>%
    inner_join(executive) %>%
    select(executive_id, large_photo_path) %>%
    filter(!is.na(large_photo_path)) %>%
    compute(name="equilar_photos_old", temporary=FALSE)

rs <- dbGetQuery(pg, "ALTER TABLE equilar_photos_old OWNER TO director_photo")

equilar_photos_old %>%
    select(executive_id) %>%
    distinct() %>%
    count()

# Code to add images to Git LFS tracking
extensions <-
    executive %>%
    select(large_photo_path) %>%
    mutate(extension = regexp_replace(large_photo_path, "^.*\\.", ""))

extensions %>%
    count(extension)

add_to_lfs <- function(extension) {
    system(paste('git lfs track "*.', extension, '"'))
}

extensions %>%
    select(extension) %>%
    distinct() %>%
    as_data_frame() %>%
    .[[1]] %>%
    lapply(., add_to_lfs)

# Get list of pictures to download
partial_path <-
    executive %>%
    filter(!is.na(large_photo_path)) %>%
    select(large_photo_path) %>%
    collect(n=Inf) %>%
    .[[1]]

# Function to download images
download_image <- function(partial_path) {
    local_path <- file.path("photos/equilar", partial_path)
    res <- TRUE
    if(!file.exists(local_path)) {
        full_path <- file.path("http://atlas.equilar.com/images", partial_path)
        dir.create(dirname(local_path), showWarnings = FALSE)
        res <- download.file(full_path, local_path)
    }
    return(res)
}

# Download images
library(parallel)
res <- unlist(mclapply(partial_path, download_image, mc.cores = 8))
table(res)
