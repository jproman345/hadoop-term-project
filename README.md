# Investigating MLB Park Advantages Using Hadoop MapReduce
## Project Overview

This project explores how the varying dimensions of Major League Baseball (MLB) stadiums influence gameplay strategies and outcomes. Using Hadoop MapReduce, we processed large datasets of pitch-by-pitch data and ballpark dimensions to identify trends and correlations between stadium features and player performance. We analyzed over 300,000 pitches from the 2023 MLB season and how park dimensions impact hitting, pitching strategies, and defensive positioning.
## Key Objectives
    *Analyze how stadium dimensions affect gameplay outcomes, such as home runs, triples, and strikeouts.
    *Use data analytics to uncover insights into the strategic adjustments players and teams make based on park size.
    *Investigate how environmental factors, like altitude, contribute to game dynamics.

## Methodology
    *Data Collection: We scraped pitch-by-pitch data from baseballsavant.mlb.com and gathered ballpark dimensions from Seamheads.com. We processed these datasets using Hadoop MapReduce.
    *Data Processing: We combined the datasets through several MapReduce jobs, extracted key information such as ballpark name, hit distance, and pitch type, and ran analyses on the relationship between ballpark size and gameplay outcomes.
    *Analysis: We examined how park dimensions influenced player performance, particularly focusing on trends in home runs and extra-base hits in different stadiums.

## Results and Insights
    *Park Dimensions: Larger outfields, such as Kauffman Stadium, lead to more extra-base hits, while smaller parks, like Yankee Stadium, see more home runs.
    *Environmental Factors: High-altitude parks, like Coors Field, see increased home run distances due to thinner air.
    *Strategic Adjustments: Players and teams adapt their strategies based on stadium characteristics, affecting their approach to both hitting and pitching.

## Future Work

Further research can extend to multi-season analysis and factor in additional environmental variables such as temperature and weather conditions to provide even more granular insights for teams and analysts.
Authors

Project by Jason Roman & Michael McMahon
