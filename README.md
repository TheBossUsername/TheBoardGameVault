# The Board Game Vault

There's nothing quite like bringing a game to a party and having it become someone's new favorite.

As one who loves board games, I know firsthand how challenging it can be to discover new games that suit your preferences.

- Have you ever tried to find the perfect board game for your event, but only came across blogs and personal opinions?
- Have you ever wanted a reliable list of games based on your favorite game mechanics?
- And even if you found such lists, did you struggle to navigate the excessive content and links on websites?

To solve these problems, I created a regularly updated database of board games made available on my website [TheBoardGameVault.com](https://theboardgamevault.com). Games are displayed in a list format, allowing you to locate the top games based on your preferences such as the number of players, game mechanics, categories, and more.

## Chapter 1: Creating a Database

When creating a website about the best board games, the first step is to gather data. But where can you find data about board games? After conducting a quick search, it becomes clear that the ultimate source for all things related to board games is [BoardGameGeek](https://boardgamegeek.com). They have information on over 150,000 board games, with more than 20 unique fields for each game.

After identifying the source, I started looking into how to legally retrieve the data regularly from their website and store it in my database. Fortunately, BoardGameGeek offers a database of game rankings and an API to access more information about each game. So, I had the source and a method to extract the data.

The database we had was great, but it only provided limited data for each game and did not list an overall rank for every game. To assign a rank to each entry, I created a Python script to sort the CSV (Comma Separated Value) file and assign ranks to the unranked games. First, it sorts by Bayes average rating, then by average, and finally in the order they were listed. After assigning an overall rank to all the games, the script uses the IDs to call the API and retrieve information for each board game one by one, then enters the information into the SQL database.

## Chapter 2: Creating The Website

After gathering the information, I needed to display it on a website. I started by creating a website using HTML, CSS, and JavaScript. I programmed it to showcase the board games in a ranked list and added a few filters. However, even with just a few filters, the website took around 30 seconds to load.

I went to Tableau to create data visualizations for a detailed view of the game. As I started learning how to use Tableau, I quickly realized how intuitive and easy it was to make filters, create charts, and build great-looking dashboards with just a click of a button. I would be reinventing the wheel by spending hours creating filters in JavaScript when I could have a ready-to-deploy filter in 2 seconds.

I shifted my entire website design approach from using JavaScript to displaying a Tableau dashboard. Although JavaScript offers more flexibility and customization, for the large volumes of data I was handling, Tableau proved to be faster at processing.

If you want to view the half-finished JavaScript version of the website, [Old Website](https://thebossusername.github.io/Senior-Project/).
Or the Github Repository here [Repository](https://github.com/TheBossUsername/Senior-Project).

Tableau is excellent for creating beautiful charts, but it's not as effective for generating lists. After conducting extensive research and making small adjustments over time, I was able to create an appealing dashboard featuring filterable board games and a detailed individual view.

## Conclusion

TheBoardGameVault.com is a continually evolving project that aims to make finding the perfect board game as easy as possible. With regularly updated data, intuitive filters, and a focus on user preferences, it's a valuable tool for any board game enthusiast.

Visit the website at [TheBoardGameVault.com](https://theboardgamevault.scott.it.com) and discover your next favorite game!
