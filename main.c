// ============================================================
// ESTEIRA INDUSTRIAL - ESP32 + FreeRTOS
// Sistemas em Tempo Real
// ESP-IDF 6.1
//
// Tasks:
// - ENC_SENSE  : periódica 5 ms -> notifica SPD_CTRL
// - SPD_CTRL   : controle PI simulado + HMI soft
// - SORT_ACT   : evento Touch B -> desviador
// - SAFETY     : evento Touch D -> E-stop
// ============================================================


// ============================================================
// BIBLIOTECAS
// ============================================================

#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"

#include "esp_timer.h"
#include "esp_log.h"
#include "sdkconfig.h"

#include "driver/gpio.h"
#include "driver/touch_sens.h"


// ============================================================
// CONFIGURACOES GERAIS
// ============================================================

#define TAG "ESTEIRA"


// ============================================================
// TOUCH PADS
// ============================================================

#define TP_OBJ      7       // Touch B -> GPIO27 -> objeto
#define TP_HMI      8       // Touch C -> GPIO33 -> HMI
#define TP_ESTOP    9       // Touch D -> GPIO32 -> E-stop

#define TOUCH_THRESHOLD 1700


// ============================================================
// TEMPORIZACAO
// ============================================================

#define ENC_T_MS 5

#define DEADLINE_ENC_US       5000
#define DEADLINE_CTRL_US     10000
#define DEADLINE_SORT_US     10000
#define DEADLINE_SAFETY_US    5000


// ============================================================
// CONFIGURACAO DO CENARIO DE TESTE
// ============================================================

// Politicas disponíveis
#define POLITICA_DM      1
#define POLITICA_CUSTOM  2
#define POLITICA_RM      3

// ============================================================
// ALTERAR SOMENTE ESTA LINHA PARA TROCAR POLITICA
// ============================================================

#define POLITICA_ATUAL POLITICA_CUSTOM //POLITICA_CUSTOM //POLITICA_RM


// ============================================================
// PRIORIDADES CONFORME POLITICA
// ============================================================

#if POLITICA_ATUAL == POLITICA_DM

    #define TESTE_POLITICA "DM"

    // Deadline Monotonic:
    // menor deadline = maior prioridade
    //
    // SAFETY    -> D = 5 ms
    // ENC_SENSE -> D = 5 ms
    // SPD_CTRL  -> D = 10 ms
    // SORT_ACT  -> D = 10 ms

    #define PRIO_ESTOP  5
    #define PRIO_ENC    5
    #define PRIO_CTRL   4
    #define PRIO_SORT   4


#elif POLITICA_ATUAL == POLITICA_CUSTOM

    #define TESTE_POLITICA "CUSTOM"

    // Prioridades definidas segundo criticidade:
    // SAFETY > ENC_SENSE > SPD_CTRL > SORT_ACT

    #define PRIO_ESTOP  5
    #define PRIO_ENC    4
    #define PRIO_CTRL   3
    #define PRIO_SORT   2

    #elif POLITICA_ATUAL == POLITICA_RM

    #define TESTE_POLITICA "RM"

    // Rate Monotonic:
    // menor periodo / maior taxa de ativacao = maior prioridade
    //
    // ENC_SENSE -> T = 5 ms
    // SPD_CTRL  -> ativada pela ENC a cada ~5 ms
    // SORT_ACT  -> evento esporadico, intervalo minimo ~300 ms
    // SAFETY    -> evento esporadico, intervalo minimo ~300 ms

    #define PRIO_ENC    5
    #define PRIO_CTRL   5
    #define PRIO_ESTOP  4
    #define PRIO_SORT   4


#endif


// ============================================================
// IDENTIFICACAO AUTOMATICA DO CENARIO
// ============================================================

#if configUSE_PREEMPTION == 1
    #define TESTE_PREEMPCAO "ON"
#else
    #define TESTE_PREEMPCAO "OFF"
#endif

#if configUSE_TIME_SLICING == 1
    #define TESTE_TIME_SLICING "ON"
#else
    #define TESTE_TIME_SLICING "OFF"
#endif

#ifdef CONFIG_FREERTOS_UNICORE
    #define TESTE_UNICORE "ON"
#else
    #define TESTE_UNICORE "OFF"
#endif


#define TESTE_FREQ_MHZ CONFIG_ESP_DEFAULT_CPU_FREQ_MHZ


// ============================================================
// STACK
// ============================================================

#define STK 3072


// ============================================================
// TIPOS DE DADOS
// ============================================================

// Evento enviado pelo touch de objeto para SORT_ACT
typedef struct
{
    int64_t t_evt_us;
} sort_evt_t;


// Estado simulado da esteira
typedef struct
{
    float rpm;
    float pos_mm;
    float set_rpm;
} belt_state_t;


// ============================================================
// HANDLES DAS TASKS
// ============================================================

static TaskHandle_t hENC  = NULL;
static TaskHandle_t hCTRL = NULL;
static TaskHandle_t hSORT = NULL;
static TaskHandle_t hSAFE = NULL;


// ENC_SENSE usa este handle para notificar SPD_CTRL
static TaskHandle_t hCtrlNotify = NULL;


// ============================================================
// MECANISMOS DE COMUNICACAO
// ============================================================

// Touch OBJ -> SORT_ACT
static QueueHandle_t qSort = NULL;


// Touch E-stop -> SAFETY
static SemaphoreHandle_t semEStop = NULL;


// Touch HMI -> SPD_CTRL
static SemaphoreHandle_t semHMI = NULL;


// ============================================================
// SENSOR TOUCH
// ============================================================

static touch_sensor_handle_t touch_sensor = NULL;

static touch_channel_handle_t touch_obj   = NULL;
static touch_channel_handle_t touch_hmi   = NULL;
static touch_channel_handle_t touch_estop = NULL;


// ============================================================
// TIMESTAMPS COMPARTILHADOS
// ============================================================

// Momento em que ENC_SENSE libera SPD_CTRL
static volatile int64_t t_ctrl_event_us = 0;


// Momento do toque de emergência
static volatile int64_t t_estop_event_us = 0;


// ============================================================
// METRICAS TEMPORAIS
// ============================================================

// ---------------- ENC_SENSE ----------------

static volatile uint32_t enc_jobs = 0;
static volatile uint32_t enc_misses = 0;

static volatile int64_t enc_jitter_max = 0;
static volatile int64_t enc_exec_max = 0;
static volatile int64_t enc_resp_max = 0;


// ---------------- SPD_CTRL ----------------

static volatile uint32_t ctrl_jobs = 0;
static volatile uint32_t ctrl_misses = 0;

static volatile int64_t ctrl_lat_max = 0;
static volatile int64_t ctrl_exec_max = 0;
static volatile int64_t ctrl_resp_max = 0;


// ---------------- SORT_ACT ----------------

static volatile uint32_t sort_jobs = 0;
static volatile uint32_t sort_misses = 0;

static volatile int64_t sort_lat_max = 0;
static volatile int64_t sort_exec_max = 0;
static volatile int64_t sort_resp_max = 0;


// ---------------- SAFETY ----------------

static volatile uint32_t safety_jobs = 0;
static volatile uint32_t safety_misses = 0;

static volatile int64_t safety_lat_max = 0;
static volatile int64_t safety_exec_max = 0;
static volatile int64_t safety_resp_max = 0;


// ============================================================
// ESTADO SIMULADO DA ESTEIRA
// ============================================================

static belt_state_t g_belt =
{
    .rpm = 0.0f,
    .pos_mm = 0.0f,
    .set_rpm = 120.0f
};


// ============================================================
// FUNCOES AUXILIARES
// ============================================================

// Busy loop previsivel para simular custo computacional
static inline void cpu_tight_loop_us(uint32_t us)
{
    int64_t start = esp_timer_get_time();

    while ((esp_timer_get_time() - start) < us)
    {
        __asm__ __volatile__("nop");
    }
}


// ============================================================
// CALLBACK DA INTERRUPCAO TOUCH
// ============================================================

static bool touch_callback(
    touch_sensor_handle_t sens_handle,
    const touch_hw_active_event_data_t *event,
    void *user_ctx)
{
    (void)sens_handle;
    (void)user_ctx;

    BaseType_t task_woken = pdFALSE;

    static int64_t ultimo_obj = 0;
    static int64_t ultimo_hmi = 0;
    static int64_t ultimo_estop = 0;

    // Debounce para evitar multiplas ativacoes em um unico toque
    const int64_t DEBOUNCE_US = 300000;   // 300 ms

    int64_t agora = esp_timer_get_time();


    // ========================================================
    // TOUCH B - OBJETO - GPIO27
    // ========================================================

    if (((event->active_mask & (1U << TP_OBJ)) != 0U) &&
        ((agora - ultimo_obj) > DEBOUNCE_US))
    {
        ultimo_obj = agora;

        sort_evt_t ev =
        {
            .t_evt_us = agora
        };

        xQueueSendFromISR(
            qSort,
            &ev,
            &task_woken
        );
    }


    // ========================================================
    // TOUCH C - HMI - GPIO33
    // ========================================================

    if (((event->active_mask & (1U << TP_HMI)) != 0U) &&
        ((agora - ultimo_hmi) > DEBOUNCE_US))
    {
        ultimo_hmi = agora;

        xSemaphoreGiveFromISR(
            semHMI,
            &task_woken
        );
    }


    // ========================================================
    // TOUCH D - E-STOP - GPIO32
    // ========================================================

    if (((event->active_mask & (1U << TP_ESTOP)) != 0U) &&
        ((agora - ultimo_estop) > DEBOUNCE_US))
    {
        ultimo_estop = agora;

        // Guarda instante do evento para calcular latencia
        t_estop_event_us = agora;

        xSemaphoreGiveFromISR(
            semEStop,
            &task_woken
        );
    }


    return task_woken == pdTRUE;
}


// ============================================================
// INICIALIZACAO DO TOUCH
// ============================================================

static void init_touch(void)
{
    // Configuracao de amostragem
    touch_sensor_sample_config_t sample_cfg =
        TOUCH_SENSOR_V1_DEFAULT_SAMPLE_CONFIG(
            5.0,
            TOUCH_VOLT_LIM_L_0V5,
            TOUCH_VOLT_LIM_H_1V7
        );


    // Configuracao do controlador
    touch_sensor_config_t sensor_cfg =
        TOUCH_SENSOR_DEFAULT_BASIC_CONFIG(
            1,
            &sample_cfg
        );


    ESP_ERROR_CHECK(
        touch_sensor_new_controller(
            &sensor_cfg,
            &touch_sensor
        )
    );


    // Threshold definido experimentalmente
    touch_channel_config_t channel_cfg =
    {
        .abs_active_thresh = { TOUCH_THRESHOLD },
        .charge_speed = TOUCH_CHARGE_SPEED_7,
        .init_charge_volt = TOUCH_INIT_CHARGE_VOLT_DEFAULT,
        .group = TOUCH_CHAN_TRIG_GROUP_BOTH,
    };


    // Canal 7 - GPIO27
    ESP_ERROR_CHECK(
        touch_sensor_new_channel(
            touch_sensor,
            TP_OBJ,
            &channel_cfg,
            &touch_obj
        )
    );


    // Canal 8 - GPIO33
    ESP_ERROR_CHECK(
        touch_sensor_new_channel(
            touch_sensor,
            TP_HMI,
            &channel_cfg,
            &touch_hmi
        )
    );


    // Canal 9 - GPIO32
    ESP_ERROR_CHECK(
        touch_sensor_new_channel(
            touch_sensor,
            TP_ESTOP,
            &channel_cfg,
            &touch_estop
        )
    );


    // Filtro requerido pelo Touch V1
    touch_sensor_filter_config_t filter_cfg =
        TOUCH_SENSOR_DEFAULT_FILTER_CONFIG();


    filter_cfg.interval_ms = 50;


    ESP_ERROR_CHECK(
        touch_sensor_config_filter(
            touch_sensor,
            &filter_cfg
        )
    );


    // Callback da interrupcao
    touch_event_callbacks_t callbacks =
    {
        .on_hw_active = touch_callback,
    };


    ESP_ERROR_CHECK(
        touch_sensor_register_callbacks(
            touch_sensor,
            &callbacks,
            NULL
        )
    );


    ESP_ERROR_CHECK(
        touch_sensor_enable(touch_sensor)
    );


    ESP_ERROR_CHECK(
        touch_sensor_start_continuous_scanning(
            touch_sensor
        )
    );


    ESP_LOGI(
        TAG,
        "Touchs inicializados: OBJ=GPIO27, HMI=GPIO33, ESTOP=GPIO32"
    );
}


// ============================================================
// ENC_SENSE
// Periodica a cada 5 ms
// Estima velocidade e posicao
// ============================================================

static void task_enc_sense(void *arg)
{
    (void)arg;

    TickType_t next = xTaskGetTickCount();

    const TickType_t T =
        pdMS_TO_TICKS(ENC_T_MS);

    const int64_t PERIOD_US =
        ENC_T_MS * 1000LL;


    // Instante esperado da primeira ativacao
    int64_t release_esperado_us =
        esp_timer_get_time();


    for (;;)
    {
        int64_t t_start_us =
            esp_timer_get_time();


        // Jitter:
        // inicio real - release esperado
        int64_t jitter_us =
            t_start_us - release_esperado_us;

        int64_t jitter_abs_us =
            (jitter_us >= 0)
            ? jitter_us
            : -jitter_us;


        // ====================================================
        // DINAMICA SIMULADA DA ESTEIRA
        // ====================================================

        float err =
            g_belt.set_rpm - g_belt.rpm;


        // Simula inercia
        g_belt.rpm +=
            0.05f * err;


        // Posicao simulada
        g_belt.pos_mm +=
            (g_belt.rpm / 60.0f) *
            (ENC_T_MS / 1000.0f) *
            100.0f;


        // Carga computacional aproximada de 0.7 ms
        cpu_tight_loop_us(700);


        int64_t t_end_us =
            esp_timer_get_time();


        int64_t exec_us =
            t_end_us - t_start_us;


        // Resposta relativa ao instante esperado de release
        int64_t resposta_us =
            t_end_us - release_esperado_us;


        // ====================================================
        // METRICAS
        // ====================================================

        enc_jobs++;


        if (resposta_us > DEADLINE_ENC_US)
        {
            enc_misses++;
        }


        if (jitter_abs_us > enc_jitter_max)
        {
            enc_jitter_max = jitter_abs_us;
        }


        if (exec_us > enc_exec_max)
        {
            enc_exec_max = exec_us;
        }


        if (resposta_us > enc_resp_max)
        {
            enc_resp_max = resposta_us;
        }


        // ====================================================
        // LIBERA SPD_CTRL
        // ====================================================

        t_ctrl_event_us = t_end_us;


        if (hCtrlNotify != NULL)
        {
            xTaskNotifyGive(hCtrlNotify);
        }


        // Proximo release teorico
        release_esperado_us += PERIOD_US;


        // Mantem periodicidade
        vTaskDelayUntil(
            &next,
            T
        );
    }
}


// ============================================================
// SPD_CTRL
// Encadeada pela ENC_SENSE
// Controle PI + HMI soft
// ============================================================

static void task_spd_ctrl(void *arg)
{
    (void)arg;

    float kp = 0.4f;
    float ki = 0.1f;
    float integ = 0.0f;


    for (;;)
    {
        // Aguarda ENC_SENSE
        ulTaskNotifyTake(
            pdTRUE,
            portMAX_DELAY
        );


        int64_t evento_us =
            t_ctrl_event_us;


        int64_t t_start_us =
            esp_timer_get_time();


        // Latencia desde a liberacao pela ENC
        int64_t latencia_us =
            t_start_us - evento_us;


        // ====================================================
        // CONTROLE PI - PARTE HARD
        // ====================================================

        float err =
            g_belt.set_rpm - g_belt.rpm;


        integ +=
            err * (ENC_T_MS / 1000.0f);


        float u =
            kp * err + ki * integ;


        // Saturacao
        if (u > 50.0f)
        {
            u = 50.0f;
        }


        if (u < -50.0f)
        {
            u = -50.0f;
        }


        // Atua sobre a velocidade simulada
        g_belt.rpm +=
            0.02f * u;


        // Carga computacional aproximada de 1.2 ms
        cpu_tight_loop_us(1200);


        int64_t t_end_us =
            esp_timer_get_time();


        int64_t exec_us =
            t_end_us - t_start_us;


        int64_t resposta_us =
            t_end_us - evento_us;


        // ====================================================
        // METRICAS
        // ====================================================

        ctrl_jobs++;


        if (resposta_us > DEADLINE_CTRL_US)
        {
            ctrl_misses++;
        }


        if (latencia_us > ctrl_lat_max)
        {
            ctrl_lat_max = latencia_us;
        }


        if (exec_us > ctrl_exec_max)
        {
            ctrl_exec_max = exec_us;
        }


        if (resposta_us > ctrl_resp_max)
        {
            ctrl_resp_max = resposta_us;
        }


        // ====================================================
        // HMI - PARTE SOFT
        // ====================================================

        if (xSemaphoreTake(semHMI, 0) == pdTRUE)
        {
            ESP_LOGI(
                TAG,
                "HMI: rpm=%.1f set=%.1f pos=%.1fmm",
                g_belt.rpm,
                g_belt.set_rpm,
                g_belt.pos_mm
            );


            // Carga adicional soft aproximada de 0.4 ms
            cpu_tight_loop_us(400);
        }
    }
}


// ============================================================
// SORT_ACT
// Touch B - desvio de objeto
// Deadline = 10 ms
// ============================================================

static void task_sort_act(void *arg)
{
    (void)arg;

    sort_evt_t ev;


    for (;;)
    {
        if (xQueueReceive(
                qSort,
                &ev,
                portMAX_DELAY) == pdTRUE)
        {
            // Inicio efetivo da task
            int64_t t_start_us =
                esp_timer_get_time();


            // Latencia:
            // inicio - instante do toque
            int64_t latencia_us =
                t_start_us - ev.t_evt_us;


            // Simula acionamento do desviador
            // Carga aproximada = 0.7 ms
            cpu_tight_loop_us(700);


            int64_t t_end_us =
                esp_timer_get_time();


            int64_t exec_us =
                t_end_us - t_start_us;


            // Resposta completa:
            // fim - instante do toque
            int64_t resposta_us =
                t_end_us - ev.t_evt_us;


            // =================================================
            // METRICAS
            // =================================================

            sort_jobs++;


            bool missed =
                resposta_us > DEADLINE_SORT_US;


            if (missed)
            {
                sort_misses++;
            }


            if (latencia_us > sort_lat_max)
            {
                sort_lat_max = latencia_us;
            }


            if (exec_us > sort_exec_max)
            {
                sort_exec_max = exec_us;
            }


            if (resposta_us > sort_resp_max)
            {
                sort_resp_max = resposta_us;
            }


            // Log individual do evento
            ESP_LOGI(
                TAG,
                "SORT_ACT | lat=%" PRId64
                " us | exec=%" PRId64
                " us | resp=%" PRId64
                " us | deadline=%s",
                latencia_us,
                exec_us,
                resposta_us,
                missed ? "MISS" : "OK"
            );
        }
    }
}


// ============================================================
// SAFETY
// Touch D - parada de emergencia
// Deadline = 5 ms
// ============================================================

static void task_safety(void *arg)
{
    (void)arg;


    for (;;)
    {
        if (xSemaphoreTake(
                semEStop,
                portMAX_DELAY) == pdTRUE)
        {
            int64_t t_start_us =
                esp_timer_get_time();


            // Latencia desde o toque
            int64_t latencia_us =
                t_start_us - t_estop_event_us;


            // =================================================
            // ACAO CRITICA DE SEGURANCA
            // =================================================

            // Zera referencia de velocidade
            g_belt.set_rpm = 0.0f;


            // Carga simulada aproximada de 0.9 ms
            cpu_tight_loop_us(900);


            int64_t t_end_us =
                esp_timer_get_time();


            int64_t exec_us =
                t_end_us - t_start_us;


            int64_t resposta_us =
                t_end_us - t_estop_event_us;


            // =================================================
            // METRICAS
            // =================================================

            safety_jobs++;


            bool missed =
                resposta_us > DEADLINE_SAFETY_US;


            if (missed)
            {
                safety_misses++;
            }


            if (latencia_us > safety_lat_max)
            {
                safety_lat_max = latencia_us;
            }


            if (exec_us > safety_exec_max)
            {
                safety_exec_max = exec_us;
            }


            if (resposta_us > safety_resp_max)
            {
                safety_resp_max = resposta_us;
            }


            ESP_LOGI(
                TAG,
                "SAFETY | lat=%" PRId64
                " us | exec=%" PRId64
                " us | resp=%" PRId64
                " us | deadline=%s",
                latencia_us,
                exec_us,
                resposta_us,
                missed ? "MISS" : "OK"
            );
        }
    }
}


// ============================================================
// ZERA METRICAS ANTES DA JANELA DE TESTE
// ============================================================

static void zerar_metricas(void)
{
    // Evita troca de contexto durante a copia/reset
    vTaskSuspendAll();


    // ENC_SENSE
    enc_jobs = 0;
    enc_misses = 0;

    enc_jitter_max = 0;
    enc_exec_max = 0;
    enc_resp_max = 0;


    // SPD_CTRL
    ctrl_jobs = 0;
    ctrl_misses = 0;

    ctrl_lat_max = 0;
    ctrl_exec_max = 0;
    ctrl_resp_max = 0;


    // SORT_ACT
    sort_jobs = 0;
    sort_misses = 0;

    sort_lat_max = 0;
    sort_exec_max = 0;
    sort_resp_max = 0;


    // SAFETY
    safety_jobs = 0;
    safety_misses = 0;

    safety_lat_max = 0;
    safety_exec_max = 0;
    safety_resp_max = 0;


    xTaskResumeAll();
}

static void imprimir_ocupacao_cpu(void)
{
    char stats[2048] = {0};

    printf("\n");
    printf("============================================================\n");
    printf("              OCUPACAO DE CPU - RUN TIME STATS\n");
    printf("============================================================\n");

    vTaskGetRunTimeStats(stats);

    printf("%s\n", stats);

    printf("============================================================\n");
}


// ============================================================
// TABELA DE RESULTADOS
// ============================================================

static void imprimir_tabela_resultados(void)
{
    // ========================================================
    // COPIA DAS METRICAS
    // ========================================================

    vTaskSuspendAll();


    uint32_t e_jobs   = enc_jobs;
    uint32_t e_misses = enc_misses;

    int64_t e_jit  = enc_jitter_max;
    int64_t e_exec = enc_exec_max;
    int64_t e_resp = enc_resp_max;


    uint32_t c_jobs   = ctrl_jobs;
    uint32_t c_misses = ctrl_misses;

    int64_t c_lat  = ctrl_lat_max;
    int64_t c_exec = ctrl_exec_max;
    int64_t c_resp = ctrl_resp_max;


    uint32_t s_jobs   = sort_jobs;
    uint32_t s_misses = sort_misses;

    int64_t s_lat  = sort_lat_max;
    int64_t s_exec = sort_exec_max;
    int64_t s_resp = sort_resp_max;


    uint32_t f_jobs   = safety_jobs;
    uint32_t f_misses = safety_misses;

    int64_t f_lat  = safety_lat_max;
    int64_t f_exec = safety_exec_max;
    int64_t f_resp = safety_resp_max;


    xTaskResumeAll();


    // ========================================================
    // IDENTIFICACAO DO CENARIO
    // ========================================================

    printf("\n\n");

    printf("=============================================================================================\n");
    printf("                              CENARIO DO EXPERIMENTO\n");
    printf("=============================================================================================\n");

    printf(
        "Politica: %s | Preempcao: %s | Time slicing: %s | CPU: %d MHz | UNICORE: %s\n",
        TESTE_POLITICA,
        TESTE_PREEMPCAO,
        TESTE_TIME_SLICING,
        TESTE_FREQ_MHZ,
        TESTE_UNICORE
    );


    printf(
        "Prioridades: SAFETY=%d | ENC=%d | CTRL=%d | SORT=%d\n\n",
        PRIO_ESTOP,
        PRIO_ENC,
        PRIO_CTRL,
        PRIO_SORT
    );


    // ========================================================
    // RESULTADOS
    // ========================================================

    printf("=============================================================================================\n");
    printf("                              RESULTADOS TEMPORAIS - 30 s\n");
    printf("=============================================================================================\n");


    printf(
        "%-12s | %7s | %7s | %11s | %10s | %10s | %10s | %6s\n",
        "TASK",
        "JOBS",
        "MISSES",
        "LAT/JIT MAX",
        "EXEC MAX",
        "RESP MAX",
        "DEADLINE",
        "STATUS"
    );


    printf(
        "-------------+---------+---------+-------------+------------+------------+------------+--------\n"
    );


    // ENC_SENSE
    printf(
        "%-12s | %7" PRIu32
        " | %7" PRIu32
        " | %8" PRId64 " us"
        " | %7" PRId64 " us"
        " | %7" PRId64 " us"
        " | %7d us"
        " | %6s\n",

        "ENC_SENSE",
        e_jobs,
        e_misses,
        e_jit,
        e_exec,
        e_resp,
        DEADLINE_ENC_US,
        (e_misses == 0) ? "OK" : "MISS"
    );


    // SPD_CTRL
    printf(
        "%-12s | %7" PRIu32
        " | %7" PRIu32
        " | %8" PRId64 " us"
        " | %7" PRId64 " us"
        " | %7" PRId64 " us"
        " | %7d us"
        " | %6s\n",

        "SPD_CTRL",
        c_jobs,
        c_misses,
        c_lat,
        c_exec,
        c_resp,
        DEADLINE_CTRL_US,
        (c_misses == 0) ? "OK" : "MISS"
    );


    // SORT_ACT
    printf(
        "%-12s | %7" PRIu32
        " | %7" PRIu32
        " | %8" PRId64 " us"
        " | %7" PRId64 " us"
        " | %7" PRId64 " us"
        " | %7d us"
        " | %6s\n",

        "SORT_ACT",
        s_jobs,
        s_misses,
        s_lat,
        s_exec,
        s_resp,
        DEADLINE_SORT_US,
        (s_misses == 0) ? "OK" : "MISS"
    );


    // SAFETY
    printf(
        "%-12s | %7" PRIu32
        " | %7" PRIu32
        " | %8" PRId64 " us"
        " | %7" PRId64 " us"
        " | %7" PRId64 " us"
        " | %7d us"
        " | %6s\n",

        "SAFETY",
        f_jobs,
        f_misses,
        f_lat,
        f_exec,
        f_resp,
        DEADLINE_SAFETY_US,
        (f_misses == 0) ? "OK" : "MISS"
    );


    printf("=============================================================================================\n");

    printf(
        "LAT/JIT MAX: jitter para ENC_SENSE; latencia para as demais tasks.\n"
    );

    printf(
        "STATUS: OK = nenhum deadline perdido no periodo de teste.\n"
    );

    printf("=============================================================================================\n\n");


    fflush(stdout);
}


// ============================================================
// TASK DO RELATORIO
// ============================================================

static void task_relatorio(void *arg)
{
    (void)arg;


    // ========================================================
    // INICIO DA JANELA EXPERIMENTAL
    // ========================================================

    zerar_metricas();


    printf("\n");

    printf("============================================================\n");
    printf("          INICIO DO TESTE - JANELA DE 30 SEGUNDOS\n");
    printf("============================================================\n");

    printf(
        "Politica: %s | Preempcao: %s | CPU: %d MHz | UNICORE: %s\n",
        TESTE_POLITICA,
        TESTE_PREEMPCAO,
        TESTE_FREQ_MHZ,
        TESTE_UNICORE
    );

    printf("============================================================\n\n");


    // Aguarda 30 segundos
    vTaskDelay(
        pdMS_TO_TICKS(30000)
    );


    // Imprime resultados
    imprimir_tabela_resultados();
    imprimir_ocupacao_cpu();


    // Executa uma unica vez
    vTaskDelete(NULL);
}


// ============================================================
// APP_MAIN
// ============================================================

void app_main(void)
{
    // ========================================================
    // MECANISMOS DE COMUNICACAO
    // ========================================================

    qSort =
        xQueueCreate(
            10,
            sizeof(sort_evt_t)
        );


    semEStop =
        xSemaphoreCreateBinary();


    semHMI =
        xSemaphoreCreateBinary();


    if ((qSort == NULL) ||
        (semEStop == NULL) ||
        (semHMI == NULL))
    {
        ESP_LOGE(
            TAG,
            "Erro ao criar Queue/Semaphores"
        );

        return;
    }


    // ========================================================
    // CRIACAO DAS TASKS
    // ========================================================

    // SAFETY
    xTaskCreatePinnedToCore(
        task_safety,
        "SAFETY",
        STK,
        NULL,
        PRIO_ESTOP,
        &hSAFE,
        0
    );


    // SPD_CTRL precisa existir antes da ENC_SENSE
    xTaskCreatePinnedToCore(
        task_spd_ctrl,
        "SPD_CTRL",
        STK,
        NULL,
        PRIO_CTRL,
        &hCTRL,
        0
    );


    // Handle usado pela ENC_SENSE
    hCtrlNotify = hCTRL;


    // ENC_SENSE
    xTaskCreatePinnedToCore(
        task_enc_sense,
        "ENC_SENSE",
        STK,
        NULL,
        PRIO_ENC,
        &hENC,
        0
    );


    // SORT_ACT
    xTaskCreatePinnedToCore(
        task_sort_act,
        "SORT_ACT",
        STK,
        NULL,
        PRIO_SORT,
        &hSORT,
        0
    );


    ESP_LOGI(
        TAG,
        "Tasks e mecanismos IPC criados"
    );


    // ========================================================
    // INICIALIZACAO DO TOUCH
    // ========================================================

    init_touch();


    ESP_LOGI(
        TAG,
        "Sistema da esteira inicializado"
    );


    // ========================================================
    // JANELA EXPERIMENTAL DE 30 SEGUNDOS
    // ========================================================

    xTaskCreatePinnedToCore(
        task_relatorio,
        "RELATORIO",
        STK,
        NULL,
        1,
        NULL,
        0
    );
}