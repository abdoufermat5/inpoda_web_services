from clientApp.utils.utils import getZeepClient, loadJsonFile, FILE_PATH, preprocessingMenu


class PreprocessApp:
    def __init__(self, url='http://localhost:5000/?wsdl'):
        self.URL = url
        self.client = getZeepClient(url)
        self.service = self.client.service

        self.menu = preprocessingMenu

    def publishTweets(self):
        data = loadJsonFile(FILE_PATH)
        print("\nThis may take a while...")
        for tweet in data:
            topic = "<->".join(
                [topic["domain"].get("name") for topic in tweet.get("context_annotations",
                                                                    [{"domain": {"name": "General"}}])])
            tweet["topic"] = topic
            print(self.service.putTweet(tweet))

    def identifyAuthor(self, tweet):
        return self.service.identifyAuthor(tweet)

    def extractHashtags(self, tweet):
        return  self.service.extractTweetHashtag(tweet)

    def getSentiment(self, tweet):
        return self.service.predictSentiment(tweet)

    def identifyTopic(self, tweet):
        results = self.service.identifyTopic(tweet)
        return results

    def present_menu(self):
        self.menu()

    def run(self):
        # self.publishTweets()
        self.present_menu()
        your_choice = input("\nEnter your choice: ")
        # choice between 0 and 4
        tweets = self.service.getAllTweets()
        if 0 <= int(your_choice) <= 4:
            if your_choice == '0':
                self.publishTweets()
            elif your_choice == '1':
                print("\nIDENTIFYING AUTHOR...")
                for tweet in tweets:
                    print(self.identifyAuthor(tweet.id))
                    print("--------------------------")
            elif your_choice == '2':
                print("\nEXTRACTING HASHTAGS...")
                for tweet in tweets:
                    print("TWEET: ", tweet.text)
                    print(self.extractHashtags(tweet.id))
                    print("--------------------------")
            elif your_choice == '3':
                print("\nANALYZING SENTIMENT...")
                for tweet in tweets:
                    print("TWEET: ", tweet.text)
                    print(self.getSentiment(tweet.id))
                    print("--------------------------")
            elif your_choice == '4':
                print("\nIDENTIFYING TOPIC...")
                for tweet in tweets:
                    print("TWEET: ", tweet.text)
                    print(self.identifyTopic(tweet.id))
                    print("--------------------------")
        else:
            print("Invalid choice")
            exit(1)