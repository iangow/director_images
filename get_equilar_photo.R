library(dplyr)

pg <- src_postgres()
equilar_schema <- "executive"

executive <- tbl(pg, sql(paste0("SELECT * FROM ", equilar_schema, ".executive ")))
proxy_board_director <-
    tbl(pg, sql(paste0("SELECT * FROM ", equilar_schema, ".proxy_board_director")))

proxy_board_director %>%
    select(executive_id) %>%
    inner_join(executive) %>%
    select(executive_id, large_photo_path) %>%
    mutate(has_photo=!is.na(large_photo_path)) %>%
    group_by(executive_id) %>%
    summarize(has_photo=bool_or(has_photo)) %>%
    count(has_photo)

# Code to add images to Git LFS tracking
extensions <-
    executive %>%
    filter(!is.na(large_photo_path)) %>%
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
