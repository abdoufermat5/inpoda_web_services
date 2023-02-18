from analysis.analysisClient import AnalysisApp
from preprocess.preprocessClient import PreprocessApp
from utils.utils import main_menu

if __name__ == '__main__':
    appPreprocess = PreprocessApp()
    appAnalysis = AnalysisApp()
    try:
        main_menu()
        your_choice = input("\nEnter your choice: ")
        while your_choice != '-1':
            if your_choice == '1':
                appPreprocess.run()
            elif your_choice == '2':
                appAnalysis.run()
            else:
                print("Invalid choice!")
            main_menu()
            your_choice = input("\nEnter your choice: ")
    except Exception as e:
        print(e)
