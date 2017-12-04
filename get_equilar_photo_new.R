library(dplyr, warn.conflicts = FALSE)

pg <- src_postgres()
equilar_photos <- tbl(pg, sql("SELECT * FROM director_photo.equilar_photos"))

# Code to add images to Git LFS tracking
extensions <-
    equilar_photos %>%
    filter(!is.na(photo)) %>%
    select(photo) %>%
    mutate(extension = regexp_replace(photo, "^.*\\.", ""))

extensions %>%
    count(extension)

add_to_lfs <- function(extension) {
    cmd <- paste0('git lfs track "*.', extension, '"')
    cat(cmd, "\n")
    system(cmd)
}

extensions %>%
    select(extension) %>%
    distinct() %>%
    as_data_frame() %>%
    .[[1]] %>%
    lapply(., add_to_lfs)

# Get list of pictures to download
partial_paths <-
    equilar_photos %>%
    filter(!is.na(photo)) %>%
    select(photo) %>%
    collect(n=Inf) %>%
    mutate(photo = gsub("https://s3-us-west-2.amazonaws.com/cub-images/prod/", "", photo)) %>%
    pull()

# Function to download images
download_image <- function(partial_path) {
    local_path <- file.path("photos/equilar", partial_path)
    res <- TRUE
    if(!file.exists(local_path)) {
        full_path <- file.path("https://s3-us-west-2.amazonaws.com/cub-images/prod", partial_path)
        dir.create(dirname(local_path), showWarnings = FALSE)
        res <- download.file(full_path, local_path)
    }
    return(res)
}

# Download images
library(parallel)
system.time(res <- unlist(mclapply(partial_paths, download_image, mc.cores = 4)))
# table(res)
