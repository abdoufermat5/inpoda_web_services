from clientApp.utils.utils import getZeepClient, loadJsonFile, FILE_PATH, analysisMenu


class AnalysisApp:
    def __init__(self, url='http://localhost:5001/?wsdl'):
        self.URL = url
        self.client = getZeepClient(url)
        self.service = self.client.service
        # present menu
        self.menu = analysisMenu

    def getTopKHashTag(self, k):
        res = self.service.getTopKHashTag(k)
        for r in res:
            print(r)

    def getTopKTopic(self, k):
        res = self.service.getTopKTopic(k)
        for r in res:
            print(r)

    def getTopKUser(self, k):
        res = self.service.getTopKUser(k)
        for r in res:
            print(r)

    def getHashTagPublish(self, hashtag):
        res = self.service.publishByHashTag(hashtag)
        for r in res:
            print(r)

    def getTopicPublish(self, topic):
        res = self.service.publishByTopic(topic)
        for r in res:
            print(r)

    def getUserPublish(self, user):
        res = self.service.publishByUser(user)
        for r in res:
            print(r)

    def present_menu(self):
        self.menu()

    def run(self):
        self.present_menu()
        your_choice = input("\nEnter your choice: ")

        while your_choice != '-1':
            # choice between 1 and 6
            if 1 <= int(your_choice) <= 6:
                if your_choice == '1':
                    k = input("Enter the number of top hashtags: ")
                    self.getTopKHashTag(k)
                elif your_choice == '3':
                    k = input("Enter the number of top topics: ")
                    self.getTopKTopic(k)
                elif your_choice == '2':
                    k = input("Enter the number of top users: ")
                    self.getTopKUser(k)
                elif your_choice == '5':
                    hashtag = input("Enter the hashtag: ")
                    self.getHashTagPublish(hashtag)
                elif your_choice == '6':
                    topic = input("Enter the topic: ")
                    self.getTopicPublish(topic)
                elif your_choice == '4':
                    user = input("Enter the user: ")
                    self.getUserPublish(user)
                self.present_menu()
                your_choice = input("\nEnter your choice: ")
            else:
                print("Invalid choice")
