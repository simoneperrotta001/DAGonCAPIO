#modifiche fatte a questo file, as_json_capio in init nel workflow, e as_json_capio nel task e pre_run
import json
import time
import os

from dagon import Workflow
from dagon.task import DagonTask, TaskType


# Check if this is the main
if __name__ == '__main__':
    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo")

    # Set the dry, se è falsa allora l'esecuzione avverrà effettivamente
    workflow.set_dry(False)

    # The task a
    taskA = DagonTask(TaskType.BATCH, "A", "mkdir output;hostname > output/f1.txt")

    # The task b
    taskB = DagonTask(TaskType.BATCH, "B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt")

    # The task c
    taskC = DagonTask(TaskType.BATCH, "C", "echo $RANDOM > f3.txt; cat workflow:///A/output/f1.txt >> f3.txt")

    # The task d
    taskD = DagonTask(TaskType.BATCH, "D", "cat workflow:///B/f2.txt >> f4.txt; cat workflow:///C/f3.txt >> f4.txt")

    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    workflow.make_dependencies()

    """json.dumps è un metodo in Python che converte un oggetto Python in una stringa JSON. Nel contesto del codice fornito, json.dumps(jsonWorkflow, sort_keys=True, indent=2) 
    prende l'oggetto Python jsonWorkflow (che è una rappresentazione del workflow in formato JSON) e lo converte in una stringa JSON.
    sort_keys=True indica di ordinare le chiavi del dizionario in ordine alfabetico nella stringa JSON risultante.
    indent=2 specifica di indentare la stringa JSON con 2 spazi per rendere la struttura più leggibile.
    Quindi, stringWorkflow conterrà la rappresentazione JSON del workflow come una stringa. Questa stringa viene poi scritta nel file di output."""
    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    jsonCapioWorkflow = workflow.as_json_capio()
    with open('dataflow-demo-capio.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonCapioWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # run the workflow
    workflow.run()

    """va a prendersi i risultati del workflow se questo non è stato simulato, prendendo la directory del task d, concatenandoci
    il file f3.txt. Se il file ancora non esiste da modo che prima venga creato dormendo."""
    if workflow.get_dry() is False:
        # set the result filename
        result_filename = taskD.get_scratch_dir() + "/f4.txt"
        while not os.path.exists(result_filename):
            time.sleep(1)
        """apre il file dove sono contenuti i risultati e se li mette in un'unica volta in result, che poi viene completamente
        stampato a schermo."""
        # get the results
        with open(result_filename, "r") as infile:
            result = infile.readlines()
            print(result)


"""import os
import subprocess

# Imposta la variabile di ambiente LD_PRELOAD
os.environ["LD_PRELOAD"] = "/home/s.perrotta/capio/build/libcapio_posix.so"

# Esegui il programma Python desiderato
subprocess.run(["python", "dataflow-demo-remote.py"])
"""


"""
#include <Python.h>

int run_python_script(const char *script_path) {
    // Inizializza l'interprete Python
    Py_Initialize();

    // Imposta il percorso di ricerca dei moduli Python
    // Supponiamo che i tuoi moduli Python siano in una directory "modules"
    PySys_SetPath("path/to/your/modules/directory");

    // Carica lo script Python
    FILE *file = fopen(script_path, "r");
    if (file == NULL) {
        fprintf(stderr, "Errore nell'apertura dello script Python\n");
        return 1;
    }

    PyRun_SimpleFile(file, script_path);

    // Chiudi il file
    fclose(file);

    // Rilascia le risorse dell'interprete Python
    Py_Finalize();

    return 0;
}

#include <stdio.h>

// Dichiarazione della funzione definita altrove
int run_python_script(const char *script_path);

int main() {
    // Percorso dello script Python da eseguire
    const char *script_path = "path/to/your/script.py";

    // Esegui lo script Python
    int result = run_python_script(script_path);
    if (result != 0) {
        fprintf(stderr, "Errore durante l'esecuzione dello script Python\n");
        return 1;
    }

    return 0;
}

"""

"""gcc -o wrapper wrapper.c -I /usr/include/python3.8 -lpython3.8
"""