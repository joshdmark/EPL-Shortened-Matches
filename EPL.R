library(tidyverse)
library(data.table)
library(sqldf)
library(lubridate)
library(RCurl)

## get EPL matches
epl_matches_url <- getURL('https://raw.githubusercontent.com/joshdmark/EPL-Shortened-Matches/master/Data/epl_matches.csv')
epl_matches <- read.csv(text = epl_matches_url, stringsAsFactors = FALSE) %>% data.frame()
rm(epl_matches_url) ## remove url 

## get EPL goals
epl_goals_url <- getURL('https://raw.githubusercontent.com/joshdmark/EPL-Shortened-Matches/master/Data/epl_goals.csv')
epl_goals <- read.csv(text = epl_goals_url, stringsAsFactors = FALSE) %>% data.frame()
rm(epl_goals_url) ## remove url 

## get EPL team ids
epl_teams_url <- getURL('https://raw.githubusercontent.com/joshdmark/EPL-Shortened-Matches/master/Data/epl_team_ids.csv')
epl_teams <- read.csv(text = epl_teams_url, stringsAsFactors = FALSE) %>% data.frame()
rm(epl_teams_url) ## remove url 

## fix match times (format as timestamps)
epl_matches <- epl_matches %>% mutate(game_dt = dmy_hm(game_dt))
epl_goals <- epl_goals %>% mutate(game_dt = dmy_hm(game_dt))

## add team ids to epl_matches
epl_matches <- sqldf("select em.*, et1.team_id as home_team_id, et2.team_id as away_team_id
             from epl_matches em 
             left join epl_teams et1 on em.home_team = et1.team_name 
             left join epl_teams et2 on em.away_team = et2.team_name")

## add team ids to epl goals 
epl_goals <- sqldf("select eg.*, et1.team_id as home_team_id, et2.team_id as away_team_id
             from epl_goals eg 
                     left join epl_teams et1 on eg.home_team = et1.team_name 
                     left join epl_teams et2 on eg.away_team = et2.team_name")

## create league_results tables 
league_results <- data.frame()
league_results_75 <- data.frame()
league_results_60 <- data.frame()
league_goals <- data.frame()

## loop through all teams 
for (team in 1:nrow(epl_teams)){
  team_name <- epl_teams[team, 'team_name']
  team_id <- epl_teams[team, 'team_id']
  
  ## print team names for loop progress 
  print(team_name)
  print(team_id)
  
  ## create df of each team's games 
  team_games <- epl_goals %>%
    filter(home_team == team_name | away_team == team_name) %>%
    mutate(team_name = team_name,
           opp_name = ifelse(home_team == team_name, away_team, home_team),
           team_id = team_id, 
           opp_team_id = ifelse(home_team == team_name, away_team_id, home_team_id),
           team_goal = ifelse(goal_scorer_team == team_id, goal_ind, 0),
           opp_goal = ifelse(goal_scorer_team == opp_team_id, goal_ind, 0)) %>% 
    ## if games were only 75 mins
    mutate(team_goal_75 = ifelse(team_goal == 1 & goal_min <= 75, team_goal, 0), 
           opp_goal_75 = ifelse(opp_goal == 1 & goal_min <= 75, opp_goal, 0)) %>% 
    ## if games were only 60 mins
    mutate(team_goal_60 = ifelse(team_goal == 1 & goal_min <= 60, team_goal, 0), 
           opp_goal_60 = ifelse(opp_goal == 1 & goal_min <= 60, opp_goal, 0))
  
  ## aggregate results for each game (W/L/D and goals for/against)
  team_results <- team_games %>% 
    group_by(team_id, opp_team_id, game_id, game_dt, result) %>% 
    summarise(team_goals = sum(team_goal, na.rm = TRUE), 
              opp_goals = sum(opp_goal, na.rm = TRUE)) %>% 
    mutate(result = case_when(
      team_goals > opp_goals ~ 'W', 
      team_goals == opp_goals ~ 'D', 
      team_goals < opp_goals ~ 'L'), 
      points_earned = case_when(
        result == 'W' ~ 3, 
        result == 'D' ~ 1, 
        result == 'L' ~ 0)) %>% 
    arrange(game_dt)
  
  ## aggregate results for each game (W/L/D and goals for/against)
  ## 75 min games
  team_results_75 <- team_games %>% 
    group_by(team_id, opp_team_id, game_id, game_dt, result) %>% 
    summarise(team_goals = sum(team_goal_75, na.rm = TRUE), 
              opp_goals = sum(opp_goal_75, na.rm = TRUE)) %>% 
    mutate(result = case_when(
      team_goals > opp_goals ~ 'W', 
      team_goals == opp_goals ~ 'D', 
      team_goals < opp_goals ~ 'L'), 
      points_earned = case_when(
        result == 'W' ~ 3, 
        result == 'D' ~ 1, 
        result == 'L' ~ 0)) %>% 
    arrange(game_dt)
  
  ## aggregate results for each game (W/L/D and goals for/against)
  ## 60 min games
  team_results_60 <- team_games %>% 
    group_by(team_id, opp_team_id, game_id, game_dt, result) %>% 
    summarise(team_goals = sum(team_goal_60, na.rm = TRUE), 
              opp_goals = sum(opp_goal_60, na.rm = TRUE)) %>% 
    mutate(result = case_when(
      team_goals > opp_goals ~ 'W', 
      team_goals == opp_goals ~ 'D', 
      team_goals < opp_goals ~ 'L'), 
      points_earned = case_when(
        result == 'W' ~ 3, 
        result == 'D' ~ 1, 
        result == 'L' ~ 0)) %>% 
    arrange(game_dt)
  
  team_goals <- team_games %>% 
    select(team_id, team_name, opp_team_id, opp_name, game_id, game_dt, goal_min,
           goals_for = team_goal, 
           goals_against = opp_goal) %>% 
    mutate(goal_time = case_when(
      goal_min <= 15 ~ '0-15', 
      goal_min > 15 & goal_min <= 29 ~ '15 - 29', 
      goal_min > 29 & goal_min <= 45 ~ '30 - 45', 
      goal_min > 45 & goal_min <= 60 ~ '46 - 60', 
      goal_min > 60 & goal_min <= 75 ~ '61 - 75', 
      goal_min > 75 ~ 'after 75'
    )) 
  
  ## combine team_results & league_results
  league_results <- suppressWarnings(bind_rows(league_results, team_results))
  league_results_75 <- suppressWarnings(bind_rows(league_results_75, team_results_75))
  league_results_60 <- suppressWarnings(bind_rows(league_results_60, team_results_60))
  league_goals <- suppressWarnings(bind_rows(league_goals, team_goals))
  
}

## actual EPL table
league_table <- league_results %>% 
  mutate(W = as.numeric(result == 'W'), 
         D = as.numeric(result == 'D'), 
         L = as.numeric(result == 'L')) %>% 
  group_by(team_id) %>% 
  summarise(pts = sum(points_earned), 
            gf = sum(team_goals), 
            ga = sum(opp_goals), 
            W = sum(W),
            D = sum(D),
            L = sum(L)) %>% 
  mutate(goal_diff = gf - ga) %>% 
  arrange(desc(pts), desc(goal_diff)) %>% 
  mutate(standing = 1:n()) %>% 
  select(standing, team_id, pts, W, D, L, gf, ga, goal_diff)

## table if games were only 75 mins
league_table_75 <- league_results_75 %>% 
  mutate(W_75 = as.numeric(result == 'W'), 
         D_75 = as.numeric(result == 'D'), 
         L_75 = as.numeric(result == 'L')) %>% 
  group_by(team_id) %>% 
  summarise(pts = sum(points_earned), 
            gf = sum(team_goals), 
            ga = sum(opp_goals), 
            W_75 = sum(W_75),
            D_75 = sum(D_75),
            L_75 = sum(L_75)) %>% 
  mutate(goal_diff = gf - ga) %>% 
  arrange(desc(pts), desc(goal_diff)) %>% 
  mutate(standing = 1:n()) %>% 
  select(standing, team_id, pts, W_75, D_75, L_75, gf, ga, goal_diff)

## table if games were only 60 mins
league_table_60 <- league_results_60 %>% 
  mutate(W_60 = as.numeric(result == 'W'), 
         D_60 = as.numeric(result == 'D'), 
         L_60 = as.numeric(result == 'L')) %>% 
  group_by(team_id) %>% 
  summarise(pts = sum(points_earned), 
            gf = sum(team_goals), 
            ga = sum(opp_goals), 
            W_60 = sum(W_60),
            D_60 = sum(D_60),
            L_60 = sum(L_60)) %>% 
  mutate(goal_diff = gf - ga) %>% 
  arrange(desc(pts), desc(goal_diff)) %>% 
  mutate(standing = 1:n()) %>% 
  select(standing, team_id, pts, W_60, D_60, L_60, gf, ga, goal_diff)

## compare
compare <- sqldf("select l1.*,
                  l75.gf as gf_75, l75.ga as ga_75, l75.goal_diff as goal_diff_75, 
                      l75.pts as pts_75, l75.standing as standing_75, 
                      l1.standing - l75.standing as standing_change_75, 
                  l60.gf as gf_60, l60.ga as ga_60, l60.goal_diff as goal_diff_60, 
                      l60.pts as pts_60, l60.standing as standing_60, 
                      l1.standing - l60.standing as standing_change_60
                 from league_table l1 
                 left join league_table_75 l75 on l1.team_id = l75.team_id
                 left join league_table_60 l60 on l1.team_id = l60.team_id")

## team summaries GOALS FOR by game segments
league_goals %>% 
  group_by(team_id, goal_time) %>% 
  summarise(GF = sum(goals_for), GA = sum(goals_against)) %>% 
  select(team_id, goal_time, GF) %>% 
  group_by(team_id) %>% 
  mutate(total_GF = sum(GF)) %>% 
  ungroup() %>% 
  mutate(pct_goals = GF / total_GF) %>% 
  select(team_id, goal_time, pct_goals) %>% 
  spread(key = goal_time, value = pct_goals)

## team summaries GOALS AGAINST by game segments
league_goals %>% 
  group_by(team_id, goal_time) %>% 
  summarise(GF = sum(goals_for), GA = sum(goals_against)) %>% 
  select(team_id, goal_time, GA) %>% 
  group_by(team_id) %>% 
  mutate(total_GA = sum(GA)) %>% 
  ungroup() %>% 
  mutate(pct_goals = GA / total_GA) %>% 
  select(team_id, goal_time, pct_goals) %>% 
  spread(key = goal_time, value = pct_goals)


epl_goals %>% 
  filter(goal_ind == 1) %>% 
  # mutate(after75 = as.numeric(goal_min > 75)) %>% 
  mutate(goal_time = case_when(
    goal_min <= 15 ~ '0-15', 
    goal_min > 15 & goal_min <= 29 ~ '15 - 29', 
    goal_min > 29 & goal_min <= 45 ~ '30 - 45', 
    goal_min > 45 & goal_min <= 60 ~ '46 - 60', 
    goal_min > 60 & goal_min <= 75 ~ '61 - 75', 
    goal_min > 75 ~ 'after 75'
  )) %>% 
  # group_by(after75) %>%
  group_by(goal_time) %>% 
  summarise(goals = n()) %>% 
  mutate(pct = goals / sum(goals))

## write files for viz
fwrite(compare, 'Desktop/SPORTS/EPL_outputs/epl_table_comparison.csv')
fwrite(league_goals, 'Desktop/SPORTS/EPL_outputs/epl_league_goals.csv')