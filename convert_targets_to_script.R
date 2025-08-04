


convert_targets_to_script <- function(targets_file, output_file) {
  # Parse the file as R expressions
  wscript <- parse(targets_file)
  l_script <- length(wscript)
  exprs <- as.list(exprs[[l_script]])
  nexpr <- length(exprs)

  assignments <- vector("character", nexpr)


  heading <- deparse(wscript[1:(l_script-1)])






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

convert_targets_to_script("_targets.R", "sequential_script.R")

