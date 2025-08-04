convert_targets_to_script <- function(targets_file, output_file) {

  # Read the file as lines
  lines <- readLines(targets_file)
  # Find all lines that start with list(
  list_candidates <- grep("^\\s*list\\s*\\(", lines)
  if (length(list_candidates) == 0) stop("No list( found in file.")

  # Heuristically pick the list( that contains tar_target calls inside
  list_start <- NA
  for (idx in list_candidates) {
    # Look ahead a few lines to see if tar_target appears soon after
    lookahead <- lines[seq(idx, min(idx + 10, length(lines)))]
    if (any(grepl("tar_target\\s*\\(", lookahead))) {
      list_start <- idx
      break
    }
  }
  if (is.na(list_start)) stop("No list( containing tar_target() found in file.")

  # Everything before the list( is the heading
  heading <- lines[seq_len(list_start - 1)]

  # Parse the file as R expressions
  wscript <- parse(targets_file)
  l_script <- length(wscript)
  exprs <- as.list(wscript[[l_script]])
  nexpr <- length(exprs)

  assignments <- vector("character", nexpr)

  for (i in seq_along(exprs)) {
    expr <- exprs[[i]]
    # Check if it's a tar_target call
    if (is.call(expr) && as.character(expr[[1]]) == "tar_target") {
      target_name <- deparse(expr[[2]])
      # The second argument is the code to run
      code_expr <- expr[[3]]
      code <- paste(deparse(code_expr), collapse = "\n")
      # Format as assignment
      assignment <- paste0(target_name, " <-\n", code, "\n")
      assignments[i] <- assignment
    }
  }

  # Write heading and assignments to output file
  writeLines(c(heading, "", assignments), output_file)
}


# Example usage:

convert_targets_to_script("_targets.R", "sequential_script.R")

