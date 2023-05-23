## Project Description

The project involves setting up a daily DAG (Directed Acyclic Graph) that runs at 15:00 and retrieves data for the previous day. The DAG performs various transformations on the data as outlined below:

1. **Feed Actions Table**: In the `feed_actions` table, which contains data about the news feed in the mobile application, the DAG calculates the number of views and likes for each user's content.

2. **Message Actions Table**: In the `message_actions` table, which includes data about incoming and outgoing messages in the mobile application, the DAG calculates the number of messages received and sent by each user, as well as the number of people they message and the number of people messaging them. Each extraction is performed in a separate task.

3. **Combining Tables and Calculating Metrics**: The DAG then combines the two tables into one and calculates all the mentioned metrics for each slice based on gender, age, and operating system (OS). There are three separate tasks for each slice.

4. **Writing to ClickHouse**: The final data, including all the calculated metrics, is written to a separate table in ClickHouse.

5. **Daily Updates**: The table is updated with new data every day.

The structure of the final table is as follows:

- Date: event_date
- Slice name: dimension
- Slice value: dimension_value
- Number of views: views
- Number of likes: likes
- Number of received messages: messages_received
- Number of sent messages: messages_sent
- Number of users who received messages: users_received
- Number of users to whom messages were sent: users_sent

The slices include operating system (OS), gender, and age.

By automating these data transformations and calculations, stakeholders can gain valuable insights into user engagement, content popularity, and messaging patterns based on different dimensions.

