



convert_targets_to_script <- function(targets_file, output_file) {
  # Parse the file as R expressions
  exprs <- parse(targets_file)
  exprs <- as.list(exprs[[length(exprs)]])
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
      # assignments <- c(assignments, assignment)
      assignments[i] <- assignment
    }
  }

  writeLines(assignments, output_file)
}

# Example usage:
targets_file <- "_targets.R"
# convert_targets_to_script("targets_script.R", "sequential_script.R")
