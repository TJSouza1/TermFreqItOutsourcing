require(dplyr)
require(readr)
require(tm)
require(class)
require(slam)
require(SnowballC)
require(wordcloud)
require(ggplot2)
require(RWeka)
source("CART_ODBC_Handler.R")
options(mc.cores=1)

main <- function()
{
  load_and_init("filter.csv", "IT Outsourcing", "AllCATsAtom2016.csv")
  dtm <<- term_tokenization()
  wc_render()
}


load_and_init <- function(filter_csv, subcategory, transaction_csv)
{ #load filter terms from csv file
  filter_terms_df <- read_csv(filter_csv)  
  #load transaction data set
  transactions_df <- read_csv(transaction_csv)
  target_pscs <- subcategoryToPscLookup(subcategory)
  #write filter terms to vector
  transactions_df <<- transactions_df %>% filter(product_or_service_code %in% target_pscs)
  filter_words_vec <<- filter_terms_df %>% select(filteredTerms) %>% .$filteredTerms
}



term_tokenization <- function()
{
  BigramTokenizer1 <- function(x) 
  {
    RWeka::NGramTokenizer(x, RWeka::Weka_control(min = 3, max = 3))
  }
  
  BigramTokenizer2 <- function(x) unlist(lapply(ngrams(words(x), 2), paste, collapse = " "), use.names = FALSE)
  
 targeted_text <- transactions_df %>% select(description_of_requirement)
 review_source <- VectorSource(targeted_text)
 corpus <- Corpus(review_source)
 print("Executing content transformer")
 corpus <- tm_map(corpus, content_transformer(function(x) iconv(x, to='UTF-8-MAC', sub='byte')))
 print("Removing numbers")
 corpus <- tm_map(corpus, removeNumbers)
 print("Converting all content to lower case")
 corpus <- tm_map(corpus, content_transformer(tolower))
 print("Removing punctuation")
 corpus <- tm_map(corpus, removePunctuation)
 #corpus <- tm_map(corpus, stripWhitespace)
 print("Removing English stopwords")
 corpus <- tm_map(corpus, removeWords, stopwords("english"))
 print("Removing filter words")
 corpus <- tm_map(corpus, removeWords, filter_words_vec)
 #options(mc.cores=1)
 dtm <- DocumentTermMatrix(corpus, control = list(tokenize = BigramTokenizer2))
 dtm
}

wc_render <- function()
{
  m <- as.matrix(dtm)
  frequency <- colSums(m) ##produces vector of frequencies
  frequency <- sort(frequency, decreasing=TRUE) ##sort the frequencies
  print(frequency)
  words <- names(frequency)##write the names of the frequencies to the words vector
  word_df<- data_frame(words)
  write_csv(word_df, "wordList.csv")
  print(paste("rendering wordcloud"))
  wordcloud(words[1:100], frequency[1:1000], random.order = FALSE, random.color = TRUE)
  
  #barplot
  wf <- data.frame(word=names(frequency), freq=frequency) 
  p <- ggplot(subset(wf, freq>500), aes(word, freq))
  p <- p + geom_bar(stat="identity")
  p <- p + theme(axis.text.x=element_text(angle=45, hjust=1))  
  p
  
  row_sums <- apply(m, 1, sum)
  rowsw0terms <-which(row_sums == 0)
  m2<-m[-rowsw0terms, ]
}


