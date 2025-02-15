import time
import multiprocessing


# gemini


def tarea_en_paralelo(n):
    try:
        if n == 2:  # Simulamos un error en la tarea con n = 2
            raise ValueError("Se produjo un error en la tarea con n = 2.")
        time.sleep(n)  # Simula una tarea que toma 'n' segundos
        return f"Tarea completada en paralelo después de {n} segundos."
    except Exception as e:
        return f"Error en tarea con n={n}: {str(e)}"


if __name__ == "__main__":
    print("Iniciando tareas en paralelo...")

    num_nucleos = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=num_nucleos) as pool:
        resultados = pool.map(tarea_en_paralelo, [1, 2, 3])
    for resultado in resultados:
        print(resultado)

    print("Todas las tareas en paralelo han finalizado.")
