library(data.table)   # Fast data manipulation for large datasets
library(jsonlite)     # For reading JSON files
library(dplyr)        # For data manipulation syntax
library(lubridate)    # For date handling
library(stringr)


# ============================================================================
# 1. LOAD DATA (OPTIMIZED)
# ============================================================================

# Load CSV files with fread (much faster than read.csv)
cat("Loading user data...\n")
user_df <- fread('Financial/users_data.csv')

cat("Loading card data...\n")
card_df <- fread('Financial/cards_data.csv')

cat("Loading transaction data...\n")
transaction_df <- fread('Financial/transactions_data.csv')

cat("Loading fraud labels...\n")
train_fraud_label_df <- fread('Financial/train_fraud_labels.csv')

cat("Loading MCC codes...\n")
mcc_json <- fromJSON('Financial/mcc_codes.json')
mcc_codes_df <- data.table(
  mcc_code = names(mcc_json),
  description = as.character(mcc_json)
)


# ============================================================================
# 2. DATASET OVERVIEW
# ============================================================================

# Display number or rows and columns
cat("User data:        ", format(nrow(user_df), big.mark=","), "rows,", ncol(user_df), "columns")
cat("Card data:        ", format(nrow(card_df), big.mark=","), "rows,", ncol(card_df), "columns")
cat("Transaction data: ", format(nrow(transaction_df), big.mark=","), "rows,", ncol(transaction_df), "columns")
cat("Fraud labels:     ", format(nrow(train_fraud_label_df), big.mark=","), "rows,", ncol(train_fraud_label_df), "columns")
cat("MCC codes:        ", format(nrow(mcc_codes_df), big.mark=","), "rows,", ncol(mcc_codes_df), "columns")

# Display column names
cat("User columns:", paste(names(user_df), collapse=", "))
cat("Card columns:", paste(names(card_df), collapse=", "))
cat("Transaction columns:", paste(names(transaction_df), collapse=", "))
cat("Fraud label columns:", paste(names(train_fraud_label_df), collapse=", "))
cat("MCC code columns:", paste(names(mcc_codes_df), collapse=", "))

# ============================================================================
# 3. DATA QUALITY CHECK
# ============================================================================

# Check for duplicates (on key columns for speed)
cat("User duplicates (by id):", sum(duplicated(user_df$id)), "\n")
cat("Card duplicates (by id):", sum(duplicated(card_df$id)), "\n")
cat("Transaction duplicates (by id):", sum(duplicated(transaction_df$id)), "\n")
cat("Fraud label duplicates (by id):", sum(duplicated(train_fraud_label_df$id)), "\n")

# Missing values check
cat("\n--- Missing Values in User Data ---\n")
user_missing <- colSums(is.na(user_df))
if(sum(user_missing) == 0) {
  cat("No missing value in User Data\n")
} else {
  print(user_missing[user_missing > 0])
  cat("Missing %:", round(user_missing[user_missing > 0] / nrow(user_df) * 100, 2), "\n")
}

cat("\n--- Missing Values in Card Data ---\n")
card_missing <- colSums(is.na(card_df))
if(sum(card_missing) == 0) {
  cat("No missing value in Card Data\n")
} else {
  print(card_missing[card_missing > 0])
  cat("Missing %:", round(card_missing[card_missing > 0] / nrow(card_df) * 100, 2), "\n")
}

cat("\n--- Missing Values in Transaction Data ---\n")
# Check sample for large dataset
trans_missing <- colSums(is.na(transaction_df))
if(sum(trans_missing) == 0) {
  cat("No missing value in Transaction Data\n")
} else {
  print(trans_missing[trans_missing > 0])
  cat("Missing % (in sample):", round(trans_missing[trans_missing > 0] / nrow(transaction_df) * 100, 2), "\n")
}

cat("\n--- Missing Values in Fraud Data ---\n")
fraud_missing <- colSums(is.na(train_fraud_label_df))
if(sum(fraud_missing) == 0) {
  cat("No missing value in Fraud Data\n")
} else {
  print(fraud_missing[fraud_missing > 0])
  cat("Missing % (in sample):", round(fraud_missing[fraud_missing > 0] / nrow(train_fraud_label_df) * 100, 2), "\n")
}


# ============================================================================
# 4. DATA CLEANING
# ============================================================================

print("=== STARTING DATA CLEANING ===")

# Remove duplicates
user_df <- unique(user_df, by = "id")
card_df <- unique(card_df, by = "id")
transaction_df <- unique(transaction_df, by = "id")
train_fraud_label_df <- unique(train_fraud_label_df, by = "transaction_id")

print("Duplicates removed.")


# Set keys for faster joins 
setkey(user_df, id)
setkey(card_df, id, client_id)
setkey(transaction_df, id, client_id, card_id)
setkey(train_fraud_label_df, transaction_id)

print("Keys set for efficient joins.")


# Convert date columns
if("date" %in% names(transaction_df)) {
  transaction_df[, date := as.Date(date)]
  print("Transaction dates converted.")
}
head(transaction_df)

if("acct_open_date" %in% names(card_df)) {
  # Handle MM/YYYY format - convert to first day of the month
  card_df[, acct_open_date := as.Date(paste0("01/", acct_open_date), format = "%d/%m/%Y")]
  cat("Account open dates converted (MM/YYYY format).\n")
}
if("expires" %in% names(card_df)) {
  # Handle MM/YYYY format - convert to first day of the month
  card_df[, expires := as.Date(paste0("01/", expires), format = "%d/%m/%Y")]
  print("Expired dates converted")
}
head(card_df)


# Handle credit_limit (regression target) - remove invalid values
cat("Number of rows before removing invalid values:", nrow(card_df))
if("credit_limit" %in% names(card_df)) {
  # Remove dollar sign and convert to numeric
  card_df[, credit_limit := as.numeric(gsub("[$,]", "", credit_limit))]
  # Remove negative or zero credit limits
  card_df <- card_df[is.na(credit_limit) | credit_limit > 0]
  cat("Credit limits cleaned and converted to numeric.\n")
}
cat("Number of rows after removing invalid values (negative or zero credit limits):", nrow(card_df))
head(card_df)


# Handle fraud target (binary: YES/NO)
if("target" %in% names(train_fraud_label_df)) {
  # Convert to binary factor
  train_fraud_label_df[, target := factor(target, levels = c("No", "Yes"))]
  cat("Fraud target converted to factor.\n")
}
head(train_fraud_label_df)


### Transaction data: other columns cleaning
# 1. Clean merchant city and state (remove leading/trailing spaces)
if("merchant_city" %in% names(transaction_df)) {
  transaction_df[, merchant_city := trimws(merchant_city)]
}
if("merchant_state" %in% names(transaction_df)) {
  transaction_df[, merchant_state := trimws(merchant_state)]
}


# 2. Handle amount column - ensure numeric
if("amount" %in% names(transaction_df)) {
  # Remove dollar sign and convert to numeric
  transaction_df[, amount := as.numeric(gsub("[$,]", "", amount))]
  
  # Create flag for negative amounts (refunds/returns)
  transaction_df[, is_refund := ifelse(amount < 0, 1, 0)]
  
  cat("Transaction amounts cleaned and converted to numeric.\n")
  cat("Negative amounts (refunds):", sum(transaction_df$amount < 0, na.rm=TRUE), "\n")
}
head(transaction_df)


# 3. Handle errors column - create binary flags for each error type
if("errors" %in% names(transaction_df)) {
  cat("Processing errors column...\n")
  
  # Replace empty strings or whitespace with NA
  transaction_df[errors == "" | trimws(errors) == "", errors := NA]
  
  # Create binary flags for each error type (useful for fraud detection!)
  transaction_df[, has_error := ifelse(is.na(errors), 0, 1)]
  transaction_df[, error_bad_expiration := ifelse(grepl("Bad Expiration", errors, ignore.case = TRUE), 1, 0)]
  transaction_df[, error_bad_card_number := ifelse(grepl("Bad Card Number", errors, ignore.case = TRUE), 1, 0)]
  transaction_df[, error_insufficient_balance := ifelse(grepl("Insufficient Balance", errors, ignore.case = TRUE), 1, 0)]
  transaction_df[, error_bad_pin := ifelse(grepl("Bad PIN", errors, ignore.case = TRUE), 1, 0)]
  transaction_df[, error_bad_cvv := ifelse(grepl("Bad CVV", errors, ignore.case = TRUE), 1, 0)]
  transaction_df[, error_bad_zipcode := ifelse(grepl("Bad Zipcode", errors, ignore.case = TRUE), 1, 0)]
  transaction_df[, error_technical_glitch := ifelse(grepl("Technical Glitch", errors, ignore.case = TRUE), 1, 0)]
  
  # Count number of errors per transaction
  transaction_df[, error_count := has_error]
  transaction_df[!is.na(errors), error_count := str_count(errors, ",") + 1]
  
  cat("Error flags created:\n")
  cat("  - has_error (any error)\n")
  cat("  - error_bad_expiration\n")
  cat("  - error_bad_card_number\n")
  cat("  - error_insufficient_balance\n")
  cat("  - error_bad_pin\n")
  cat("  - error_bad_cvv\n")
  cat("  - error_bad_zipcode\n")
  cat("  - error_technical_glitch\n")
  cat("  - error_count (number of errors)\n")
  
  # Print error distribution
  cat("\nError Distribution:\n")
  cat("Transactions with errors:", sum(transaction_df$has_error), 
      "(", round(sum(transaction_df$has_error)/nrow(transaction_df)*100, 2), "%)\n")
}




### Card data: other columns cleaning
# Convert card_on_dark_web and has_chip binary columns (Yes/No) to numeric (0/1)
if("card_on_dark_web" %in% names(card_df)) {
  card_df[, card_on_dark_web := ifelse(toupper(card_on_dark_web) == "YES", 1, 0)]
  cat("card_on_dark_web converted to binary (0/1).\n")
}
card_df[, card_on_dark_web := NULL]
names(card_df)

if("has_chip" %in% names(card_df)) {
  card_df[, has_chip := ifelse(toupper(has_chip) == "YES", 1, 0)]
  cat("has_chip converted to binary (0/1).\n")
}


### User data: other columns cleaning
# Clean financial columns - remove dollar signs and convert to numeric
if("per_capita_income" %in% names(user_df)) {
  user_df[, per_capita_income := as.numeric(gsub("[$,]", "", per_capita_income))]
  cat("per_capita_income converted to numeric.\n")
}

if("yearly_income" %in% names(user_df)) {
  user_df[, yearly_income := as.numeric(gsub("[$,]", "", yearly_income))]
  cat("yearly_income converted to numeric.\n")
}

if("total_debt" %in% names(user_df)) {
  user_df[, total_debt := as.numeric(gsub("[$,]", "", total_debt))]
  cat("total_debt converted to numeric.\n")
}

# Create useful derived features for credit limit prediction
if("yearly_income" %in% names(user_df) & "total_debt" %in% names(user_df)) {
  # Debt-to-Income ratio (important for credit decisions)
  user_df[, debt_to_income_ratio := ifelse(yearly_income > 0, 
                                           total_debt / yearly_income, 
                                           NA)]
  cat("debt_to_income_ratio feature created.\n")
}

head(user_df)


# Clean MCC codes (convert to int as transaction df in interger type)
if("mcc" %in% names(transaction_df) & "mcc_code" %in% names(mcc_codes_df)) {
  # Convert mcc_code to integer to match transaction MCC
  mcc_codes_df[, mcc_code := as.integer(mcc_code)]
  cat("MCC codes formatted as integer for matching.\n")
}

head(transaction_df)
head(user_df)
head(card_df)
head(mcc_codes_df)
head(train_fraud_label_df)

# ============================================================================
# 5. CLEANED DATA SUMMARY
# ============================================================================

cat("\n=== CLEANED DATASET SUMMARY ===\n")
cat("User data:        ", format(nrow(user_df), big.mark=","), "rows\n")
cat("Card data:        ", format(nrow(card_df), big.mark=","), "rows\n")
cat("Transaction data: ", format(nrow(transaction_df), big.mark=","), "rows\n")
cat("Fraud labels:     ", format(nrow(train_fraud_label_df), big.mark=","), "rows\n")

# Basic statistics
cat("\n--- Summary Statistics ---\n")
if("credit_limit" %in% names(card_df)) {
  cat("Credit Limit (Target for Regression):\n")
  print(summary(card_df$credit_limit))
}

# Check how many extremely low/high values exist
cat("Credit limits <= $100:", sum(card_df$credit_limit <= 100, na.rm=TRUE), "\n")
cat("Credit limits >= $100,000:", sum(card_df$credit_limit >= 100000, na.rm=TRUE), "\n")

if("target" %in% names(train_fraud_label_df)) {
  cat("\nFraud Distribution (Target for Classification):\n")
  print(table(train_fraud_label_df$target))
  cat("Fraud Rate:", round(sum(train_fraud_label_df$target == "YES") / nrow(train_fraud_label_df) * 100, 2), "%\n")
}

# ============================================================================
# 6. PREPARED FRAUD DETECTION DATASET (ALL DATA MERGED)
# ============================================================================

cat("\n=== Creating Fraud Detection Dataset ===\n")

# 6.1 Merge transactions with fraud labels
fraud_data <- merge(transaction_df, train_fraud_label_df, 
                    by.x = "id", by.y = "transaction_id", 
                    all.x = TRUE)
head(fraud_data)
cat("Merged transactions with fraud labels\n")
cat("  Rows:", nrow(fraud_data), "\n")

# Remove transactions without fraud labels (unsupervised data)
fraud_data <- fraud_data[!is.na(target)]
cat("  After removing unlabeled data:", format(nrow(fraud_data), big.mark=","), "\n")
cat("  (Kept only supervised learning (labelled) data)\n")


# 6.2 Add MCC descriptions
fraud_data <- merge(fraud_data, mcc_codes_df, 
                    by.x = "mcc", by.y = "mcc_code", 
                    all.x = TRUE)
cat("Added MCC descriptions\n")
head(fraud_data)


# 6.3 Add card information
fraud_data <- merge(fraud_data, card_df, 
                    by.x = "card_id", by.y = "id", 
                    all.x = TRUE, 
                    suffixes = c("", "_card"))
cat("Merged with card data\n")


# 6.4 Add user information
fraud_data <- merge(fraud_data, user_df, 
                    by.x = "client_id", by.y = "id", 
                    all.x = TRUE, 
                    suffixes = c("", "_user"))
cat("Merged with user data\n")


# 6.5 Reorganize columns and remove duplicates
cat("Step 5: Reorganizing columns...\n")

# Remove duplicate column
if("client_id_card" %in% names(fraud_data)) {
  fraud_data[, client_id_card := NULL]
  cat("  Removed duplicate: client_id_card\n")
}

# Define column order: Transaction -> MCC -> Card -> User -> Target
transaction_cols <- c("id", "date", "client_id", "card_id", "amount", "mcc", 
                      "use_chip", "merchant_id", "merchant_city", "merchant_state", "zip",
                      "is_refund", "has_error", "error_bad_expiration", "error_bad_card_number", 
                      "error_insufficient_balance", "error_bad_pin","error_bad_cvv", 
                      "error_bad_zipcode", "error_technical_glitch", "error_count")

mcc_cols <- c("description")

card_cols <- c("card_brand", "card_type", "card_number", "expires", "cvv", "has_chip",
               "num_cards_issued", "credit_limit", "acct_open_date", "year_pin_last_changed")

user_cols <- c("current_age", "retirement_age", "birth_year", "birth_month", "gender",
               "address", "latitude", "longitude", "per_capita_income", "yearly_income",
               "total_debt", "credit_score", "num_credit_cards", "debt_to_income_ratio")

target_col <- c("target")

# Combine in desired order, keeping only columns that exist
all_cols <- c(transaction_cols, mcc_cols, card_cols, user_cols, target_col)
existing_cols <- all_cols[all_cols %in% names(fraud_data)]

# Reorder columns
setcolorder(fraud_data, existing_cols)
cat("  Columns reorganized: Transaction -> MCC -> Card -> User -> Target\n")


# Final fraud detection dataset
cat("\n--- Fraud Detection Dataset Summary ---\n")
cat("Total rows:", format(nrow(fraud_data), big.mark=","), "\n")
cat("Total columns:", ncol(fraud_data), "\n")
cat("Columns:", paste(names(fraud_data)[1:10], collapse=", "), "...\n")
head(fraud_data)


# Check fraud distribution
if("target" %in% names(fraud_data)) {
  cat("\nFraud Distribution:\n")
  print(table(fraud_data$target, useNA = "ifany"))
  cat("Fraud Rate:", round(sum(fraud_data$target == "YES", na.rm=TRUE) / 
                             sum(!is.na(fraud_data$target)) * 100, 2), "%\n")
}

# Save fraud detection dataset
saveRDS(fraud_data, "CleanedDataSet/fraud_detection_data.rds")
cat("\nFraud detection dataset saved as: fraud_detection_data.rds\n")



# ============================================================================
# 7. CREDIT LIMIT PREDICTION DATASET (USER + CARD ONLY)
# ============================================================================

cat("\n=== Creating Credit Limit Prediction Dataset ===\n")

# Merge cards with users
credit_data <- merge(card_df, user_df, 
                     by.x = "client_id", by.y = "id", 
                     all.x = TRUE, 
                     suffixes = c("_card", "_user"))

cat("Merged cards with users\n")

# Add aggregated transaction features per card

cat("\nCreating aggregated transaction features...\n")

transaction_features <- transaction_df[, .(
  total_transactions = .N,
  avg_transaction_amount = mean(amount, na.rm = TRUE),
  max_transaction_amount = max(amount, na.rm = TRUE),
  min_transaction_amount = min(amount, na.rm = TRUE),
  total_spent = sum(amount[amount > 0], na.rm = TRUE),
  total_refunded = sum(abs(amount[amount < 0]), na.rm = TRUE),
  num_refunds = sum(is_refund, na.rm = TRUE),
  transaction_frequency = .N,  
  avg_errors = mean(has_error, na.rm = TRUE),
  total_errors = sum(has_error, na.rm = TRUE)
), by = card_id]

cat("Transaction features aggregated by card_id\n")

# Merge transaction features with credit data
credit_data <- merge(credit_data, transaction_features, 
                     by.x = "id", by.y = "card_id", 
                     all.x = TRUE)

cat("Added aggregated transaction features\n")

# Final credit limit dataset
cat("\n--- Credit Limit Prediction Dataset Summary ---\n")
cat("Total rows:", format(nrow(credit_data), big.mark=","), "\n")
cat("Total columns:", ncol(credit_data), "\n")
cat("Columns:", paste(names(credit_data)[1:10], collapse=", "), "...\n")

# Check target variable
if("credit_limit" %in% names(credit_data)) {
  cat("\nCredit Limit Summary:\n")
  print(summary(credit_data$credit_limit))
}

# Remove rows with missing credit_limit 
credit_data <- credit_data[!is.na(credit_limit)]
cat("\nRows after removing missing credit_limit:", format(nrow(credit_data), big.mark=","), "\n")

# Save credit limit dataset
cat("\nSaving credit limit dataset...\n")

# Save full dataset as RDS
saveRDS(credit_data, "CleanedDataSet/credit_limit_data.rds")
fwrite(credit_data, "CleanedDataSet/credit_limit_data.csv")

