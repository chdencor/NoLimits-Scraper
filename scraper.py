from colorama import init, Fore, Style
from __init__ import createScraper

# Initialize colorama
init()

if __name__ == "__main__":
    try:
        print(Fore.CYAN + r"""
     _____      _       _        _   _ _                 _ _              
    /  __ \    (_)     | |      | | | (_)               | (_)             
    | /  \/_ __ _ _ __ | |_ ___ | | | |_ ___ _   _  __ _| |_ _______ _ __ 
    | |   | '__| | '_ \| __/ _ \| | | | / __| | | |/ _` | | |_  / _ \ '__|
    | \__/\ |  | | |_) | || (_) \ \_/ / \__ \ |_| | (_| | | |/ /  __/ |   
     \____/_|  |_| .__/ \__\___/ \___/|_|___/\__,_|\__,_|_|_/___\___|_|   
                 | |                                                      
                 |_|                                                  
         ___  ___ _ __ __ _ _ __   ___ _ __                            
        / __|/ __| '__/ _` | '_ \ / _ \ '__|                           
        \__ \ (__| | | (_| | |_) |  __/ |                                
        |___/\___|_|  \__,_| .__/ \___|_|                                
                           | |                                            
                           |_|                                           
    created by @chdencor
    """ + Style.RESET_ALL)

        print(Fore.GREEN + "Iniciando... Cerrar con Ctrl + C" + Style.RESET_ALL)
        createScraper()
    except KeyboardInterrupt:
        print("Ejecución interrumpida por el usuario. Saliendo...")
    except Exception as e:
        print(f"Ocurrió un error: {e}")
