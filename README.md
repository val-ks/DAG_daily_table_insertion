The DAG runs every day at 15:00 and retrieves data for the previous day. It performs the following transformations:

1) In the `feed_actions` table (data about the news feed in the mobile application), it calculates the number of views and likes for each user's content.
2) In the `message_actions` table (data about incoming and outgoing messages in the mobile application), it calculates the number of messages received and sent by each user, the number of people they message, and the number of people messaging them. Each extraction is performed in a separate task.
3) Afterwards it combines the two tables into one and for this combined table, it calculates all the mentioned metrics for each slice based on gender, age, and operating system (OS). There are three separate tasks for each slice. 
4) The final data including all the metrics is written to a separate table in ClickHouse.
5) Every day the table is updated with new data.

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

The slices include OS, gender, and age.
