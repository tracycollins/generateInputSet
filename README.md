# generateInputSet
Generate neural network input sets

Using the global histograms for twitter entities or types (emoji, friends, hashtags, media, userMentions, locations, images, etc), generate an input set to be used for neural network evolution.

Currently, inputs are determined based on frequency of input signal (how often it appears in a user's histograms) and how partial the signal is (must be biased to left, neutral, or right to be included).

Language analysis of the user's description may also be used (sentiment input type).

Maximum number of input types and maximum total number of inputs limits the input set size.
