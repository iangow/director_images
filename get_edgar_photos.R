library(dplyr, warn.conflicts = FALSE)
library(DBI)
library(stringr)
library(parallel)

pg <- dbConnect(RPostgres::Postgres())

photo_exists <- function(photo_url) {
    local_path  <- gsub("https://www.sec.gov/Archives/edgar/data",
                        "photos/edgar", photo_url)
    file.exists(local_path)
}

photo_matches <-
    tbl(pg, sql("SELECT * FROM director_photo.photo_matches")) %>%
    collect(n = Inf) %>%
    mutate(photo_exists = photo_exists(photo_url))

download_photo <- function(photo_url) {
    local_path  <- gsub("https://www.sec.gov/Archives/edgar/data",
                        "photos/edgar", photo_url)
    if (!dir.exists(dirname(local_path))) {
        dir.create(dirname(local_path), recursive=TRUE)
    }

    if (!file.exists(local_path)) {
        try(download.file(photo_url, local_path, method = "wget"))
    }
    return(local_path)
}

get_file_extension <- function(file_name) {
    gsub("^.*(\\..*?)$", "\\1", file_name)
}

photo_matches_to_get <-
    photo_matches %>%
    filter(!photo_exists) 

if (nrow(photo_matches_to_get) > 0) {
    photo_matches_to_get <-
        rowwise() %>%
        mutate(local_path = download_photo(photo_url)) %>%
        mutate(file_extension = get_file_extension(local_path)) %>%
        ungroup()
}


add_to_lfs <- function(extension) {
    system(paste('git lfs track "*.', extension, '"'))
}

photo_matches %>% 
    filter(!photo_exists) %>%
    mutate(extension = str_replace(photo_url, "^.*\\.", "")) %>%
    select(extension) %>%
    distinct() %>%
    pull() %>%
    lapply(add_to_lfs)

# Data originally downloaded using this code:
photo_matches$local_path <- unlist(mclapply(photo_matches$photo_url, download_photo, mc.cores=10))

photo_matches %>%
    mutate(extension = str_replace(photo_url, "^.*\\.", "")) %>%
    count(extension)
