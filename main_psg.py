import PySimpleGUI as sg
import threading
from weather_vault import Collector

# Function to build and send one table (of one data type) to the database
def one_dbTable(window, look_for, db_table_name, build_limit):
    scrapper = Collector(look_for=look_for, build_format='V1', ping_interval=60)
    for cnt in scrapper.build_df(db_table_name=db_table_name, limit=build_limit, send_to_db=True): # There are yield calls in the build_df function, with generates and iterative-like object that's used to update the ProgressBar
        window[f'-PB_{look_for}-'].UpdateBar(cnt, build_limit)          
    window.write_event_value('-THREAD DONE-', f'{db_table_name}')

# Function to execute a thread for collecting a particular data type in the background (daemon)
def populate_dbTable(look_for, db_table_name, build_limit):
    thread = threading.Thread(target=one_dbTable, name=f'{db_table_name}', args=(window, look_for, db_table_name, build_limit), daemon=True)
    thread.start()

###############################################################################################

"""

GUI-related code

"""

my_theme = {'BACKGROUND': '#38B3F0',
                'TEXT': '#000000',
                'INPUT': '#c7e78b',
                'TEXT_INPUT': '#000000',
                'SCROLL': '#c7e78b',
                'BUTTON': ('white', '#709053'),
                'PROGRESS': ('#01826B', '#D0D0D0'),
                'BORDER': 0,
                'SLIDER_DEPTH': 0,
                'PROGRESS_DEPTH': 0}
# Add your dictionary to the PySimpleGUI themes
sg.theme_add_new('My New Theme', my_theme)

# Switch your theme to use the newly added one. You can add spaces to make it more readable
sg.theme('My New Theme')

nopad = ((0, 0), (0, 0))        # ((left, right), (top, bottom))
header_layout = [
    [sg.Text(text="Data Types", size=(25, 1), background_color='orange', justification='center', pad=nopad, font=('Verdana', 10, ['bold'])), sg.Text(text="Entries", size=(8, 1), background_color='orange', justification='center', pad=nopad, font=('Verdana', 10, ['bold'])), sg.Text(text="Collection Progress", size=(63, 1), background_color='orange', pad=nopad, justification='center', font=('Verdana', 10, ['bold'])), sg.Text(text="Execute", size=(10, 1), background_color='yellow', pad=nopad, expand_x=True, justification='center', font=('Verdana', 10, ['bold']))]
]


col_layout = [
        [sg.Frame(title='', layout=header_layout)],
        [sg.Text(text="Air Temperature:", size=(28, 1), pad=((10, 0), 0)), sg.Input(size=(8, 1), key='-num_entries_temp-'), sg.ProgressBar(288, orientation='h', size=(50, 20), pad=(15, 0), key='-PB_temp-'), sg.Button(button_text="Collect", size=(9, 1), key="-collect_data_temp-", pad=((0, 5), 2), border_width=2)],
        [sg.Text(text="Rainfall:", size=(28, 1), pad=((10, 0), 0)), sg.Input(size=(8, 1), key='-num_entries_rain-'), sg.ProgressBar(288, orientation='h', size=(50, 20), pad=(15, 0), key='-PB_rain-'), sg.Button(button_text="Collect", size=(9, 1), key="-collect_data_rain-", pad=((0, 5), 2), border_width=2)],
        [sg.Text(text="Relative Humidity:", size=(28, 1), pad=((10, 0), 0)), sg.Input(size=(8, 1), key='-num_entries_humid-'), sg.ProgressBar(288, orientation='h', size=(50, 20), pad=(15, 0), key='-PB_humid-'), sg.Button(button_text="Collect", size=(9, 1), key="-collect_data_humid-", pad=((0, 5), 2), border_width=2)],
        [sg.Text(text="Wind Direction:", size=(28, 1), pad=((10, 0), 0)), sg.Input(size=(8, 1), key='-num_entries_direction-'), sg.ProgressBar(288, orientation='h', size=(50, 20), pad=(15, 0), key='-PB_direction-'), sg.Button(button_text="Collect", size=(9, 1), key="-collect_data_direction-", pad=((0, 5), 2), border_width=2)],
        [sg.Text(text="Wind Speed:", size=(28, 1), pad=((10, 0), 0)), sg.Input(size=(8, 1), key='-num_entries_speed-'), sg.ProgressBar(288, orientation='h', size=(50, 20), pad=(15, 0), key='-PB_speed-'), sg.Button(button_text="Collect", size=(9, 1), key="-collect_data_speed-", pad=((0, 5), 2), border_width=2)]
        ]

layout = [[sg.Text(text="Data Collector Controller for SGWeather", font=('Garamond', 14, ['bold', 'italic']))],
          [sg.Col(col_layout, pad=(0, 0))],
          [sg.Multiline(size=(135, 20), reroute_stdout=True, do_not_clear=False, autoscroll=True, write_only=True, key='-console-')],
          [sg.Button(button_text="Clear output", key="-clear-")]]

window = sg.Window("SGWeather - Data Collector", layout, finalize=True)

# Main programme
if __name__ == '__main__':
    while True:
        event, values = window.read()           # Programme waits here for event to be triggered
        print(event, values)

        # Check background activated events
        if event == sg.WIN_CLOSED or event == 'Exit':
            break
        elif event == '-THREAD DONE-':
            print(f"Data collection completed for {values['-THREAD DONE-']}!")

        # Check button-activated events
        if event == '-clear-':
            window['-console-'].Update('')

        if event == '-collect_data_temp-':
            num_entries = int(values['-num_entries_temp-'].strip())
            populate_dbTable(look_for='temp', db_table_name='temperature', build_limit=num_entries)
        if event == '-collect_data_rain-':
            num_entries = int(values['-num_entries_rain-'].strip())
            populate_dbTable(look_for='rain', db_table_name='rainfall', build_limit=num_entries)
        if event == '-collect_data_humid-':
            num_entries = int(values['-num_entries_humid-'].strip())
            populate_dbTable(look_for='humid', db_table_name='relative_humidity', build_limit=num_entries)
        if event == '-collect_data_direction-':
            num_entries = int(values['-num_entries_direction-'].strip())
            populate_dbTable(look_for='direction', db_table_name='wind_direction', build_limit=num_entries)
        if event == '-collect_data_speed-':
            num_entries = int(values['-num_entries_speed-'].strip())
            populate_dbTable(look_for='speed', db_table_name='wind_speed', build_limit=num_entries)

# Close the window object
window.close()