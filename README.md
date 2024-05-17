# Jared Kirschbaum Analyzing Big Data with Hadoop
# Overview
This document outlines the methodologies used for four distinct questions, each leveraging different aspects of data processing and analysis using Apache Hadoop and MapReduce. These questions analyze song data to identify various musical properties and artist characteristics.

# Usage
Run the provided `test.sh` script located in the `build` directory. All processes are initiated from the `MasterDriver.java` file, ensuring a unified execution flow for all tasks.

## Q7: Average Song Segment Analysis
This question aims to derive insights about the average characteristics of songs from a large dataset, focusing on attributes like start time, pitch, timbre, and loudness levels. I first started by identifying the max length for each input column vector. I then padded each vector with 0 values to make sure they were all of the same length. From here I was able to obtain an average vector for each column using the song.

### Data Collection
- **Source**: Million Song Dataset.

### Data Processing
- **Job 1**: Averages song segments.
- **Job 2**: Identifies the maximum vector length for normalization.
- **Job 3**: Computes the overall average vector for all songs, representing the typical song profile.

### Methods
- **Mapper and Reducer Tasks**: Used to calculate averages and normalize data.
- **Vector Calculation**: Involves adjusting all vectors to the same length and computing an average vector that typifies the dataset.

## Q8: Artist Uniqueness Analysis
This analysis determines the most unique and generic artists by calculating Euclidean distances between attribute vectors of songs. To do this I identify the average vector for each segment for each artist based on my segments from q7. This is done by grouping all the vectors associated to the arist, and their unique column from q7 and then obtaining an average vector for that feature. These average vectors were then compared to the average column segments from q7. The artist with the closest distance was considered the most common. The artist with the furtherst distance was considered the most unique.

### Data Collection
- **Source**: Million Song Dataset.

### Components
- **Data Joining**: Combines song metadata with analytical data.
- **Vector Calculation**: Standardizes vector lengths and computes average vectors for each artist.
- **Distance Calculation**: Uses Euclidean distances to assess artist uniqueness.

### Methods
- **Euclidean Distance**: The distance between two points (vectors) in Euclidean space is computed using the formula:
  $$
  d(p, q) = \sqrt{\sum_{i=1}^n (p_i - q_i)^2}
  $$
- **Processing Steps**: Includes joining datasets, averaging attributes, and calculating distances.

## Q9: Song Attribute Prediction Using Hotness Score
Predicts song attributes like tempo, time signature, and loudness based on a song's "hotness" score using regression models. To do this I cleaned the input text data and numeric values. Then based on the inputs I filled in for NaN values using 0.0 or None for an empty term array. The values were then tranied using a hotness score of 1.1 as the independant variable on a linear regression model to identify the alpha and beta coeffients to predict the features at a hotness value higher than 1.0 (the value from Q3). To identify the top terms a Bag of Words approach was used to identify the top 10 occuring terms in the dataset.

### Data Collection
- **Source**: Million Song Dataset.

### Prediction Pipeline
- **Data Preparation**: Involves handling missing values and preparing data for analysis.
- **Model Development**: Trains linear regression models to predict various song attributes.
- **Prediction and Validation**: Predicts attributes for a song with a "hotness" score of 1.1.

### Methods
- **Regression Analysis**: Utilizes linear regression to estimate song attributes from the hotness score.
- **Term Analysis**: Employs a Bag of Words model to identify common descriptive terms for artists.

## Q10: Song Recommendation System Using TF-IDF
Implements a song recommendation system based on TF-IDF scores and cosine similarity to identify songs that match user queries. To do this I cleaned the input text after merging the two provided .txt files. Then created TFIDF vectors for each document (row of data) based on the provided terms. From here the user was able to input terms of their own to identify similar songs to their preference. This was then used to obtain the TFIDF vector for the users terms and identify the top 10 closest songs by consine similarity.

### Data Integration
- **Data Joining**: Merges song analysis data with metadata.

### Data Processing
- **Term Frequency Calculation**: Calculates how often each term appears within each song's lyrics.
- **TF-IDF Computation**: Combines term frequency and inverse document frequency to weigh term importance using the formula:
  $$
  \text{TF-IDF} = (\text{Term Frequency}) \times \log\left(\frac{\text{Total Number of Documents}}{\text{Number of Documents Containing the Term}}\right)
  $$
- **Cosine Similarity Calculation**: Computes similarity scores between songs and user queries using the formula:
  $$
  \text{Cosine Similarity} = \frac{\mathbf{A} \cdot \mathbf{B}}{\|\mathbf{A}\| \|\mathbf{B}\|}



### Sorting and Ranking
- **Final Step**: Ranks songs according to their relevance to the input query terms.

## Conclusion
Each question utilizes a specific set of data processing techniques to analyze and predict various aspects of song data. These methodologies leverage the scalability and efficiency of Hadoop and MapReduce, ensuring that each task is performed effectively across large datasets.
