if (!dir.exists("data")) dir.create("data", recursive = TRUE)


require( "data.table" )

# leo el dataset
dataset <- fread("https://storage.googleapis.com/open-courses/dmeyf2025-e4a2/competencia_01_crudo.csv" )

# calculo el periodo0 consecutivo
dsimple <- dataset[, list(
  "pos" = .I,
  numero_de_cliente,
  periodo0 = as.integer(foto_mes/100)*12 +  foto_mes%%100 ) ]


# ordeno
setorder( dsimple, numero_de_cliente, periodo0 )

# calculo topes
periodo_ultimo <- dsimple[, max(periodo0) ]
periodo_anteultimo <- periodo_ultimo - 1


# calculo los leads de orden 1 y 2
dsimple[, c("periodo1", "periodo2") :=
          shift(periodo0, n=1:2, fill=NA, type="lead"),  numero_de_cliente ]

# assign most common class values = "CONTINUA"
dsimple[ periodo0 < periodo_anteultimo, clase_ternaria := "CONTINUA" ]

# calculo BAJA+1
dsimple[ periodo0 < periodo_ultimo &
           ( is.na(periodo1) | periodo0 + 1 < periodo1 ),
         clase_ternaria := "BAJA+1" ]

# calculo BAJA+2
dsimple[ periodo0 < periodo_anteultimo & (periodo0+1 == periodo1 )
         & ( is.na(periodo2) | periodo0 + 2 < periodo2 ),
         clase_ternaria := "BAJA+2" ]


# pego el resultado en el dataset original y grabo
setorder( dsimple, pos )
dataset[, clase_ternaria := dsimple$clase_ternaria ]


setorder( dataset, foto_mes, clase_ternaria, numero_de_cliente)
dataset[, .N, list(foto_mes, clase_ternaria)]

format(Sys.time(), "%a %b %d %X %Y")

# Feature Eng ########################################
# deteccion y remoción de aguinaldo ######
library(dplyr)
library(tidyr)

UMBRAL_LO <- 0.35
UMBRAL_HI <- 0.65


# 1) Baselines por cliente: medianas ene–may para CADA variable
base <- dataset %>%
  filter(foto_mes %in% c("202101","202102","202103","202104","202105")) %>%
  group_by(numero_de_cliente) %>%
  summarise(
    base_mediana_saldo   = median(mcuentas_saldo, na.rm = TRUE),
    base_mediana_payroll = median(mpayroll,        na.rm = TRUE),
    .groups = "drop"
  )

# 2) Filas de JUNIO con ambas variables a potencialmente ajustar
jun <- dataset %>%
  filter(foto_mes == "202106") %>%
  transmute(
    numero_de_cliente, foto_mes,
    jun_saldo   = mcuentas_saldo,
    jun_payroll = mpayroll
  )

# 3) Detección y ajuste INDEPENDIENTE por variable
jun_adj <- jun %>%
  left_join(base, by = "numero_de_cliente") %>%
  mutate(
    # --- mcuentas_saldo ---
    delta_saldo = jun_saldo - base_mediana_saldo,
    denom_saldo = ifelse(is.finite(base_mediana_saldo) & abs(base_mediana_saldo) > 0,
                         abs(base_mediana_saldo), NA_real_),
    ratio_saldo = delta_saldo / denom_saldo,
    aplicar_saldo = is.finite(jun_saldo) & (jun_saldo > 0) &
      is.finite(ratio_saldo) & (delta_saldo > 0) &
      ratio_saldo >= UMBRAL_LO & ratio_saldo <= UMBRAL_HI,
    mcuentas_saldo_ajustada = ifelse(aplicar_saldo,
                                     jun_saldo - 0.5 * abs(base_mediana_saldo),
                                     jun_saldo),
    
    # --- mpayroll ---
    delta_payroll = jun_payroll - base_mediana_payroll,
    denom_payroll = ifelse(is.finite(base_mediana_payroll) & abs(base_mediana_payroll) > 0,
                           abs(base_mediana_payroll), NA_real_),
    ratio_payroll = delta_payroll / denom_payroll,
    aplicar_payroll = is.finite(jun_payroll) & (jun_payroll > 0) &
      is.finite(ratio_payroll) & (delta_payroll > 0) &
      ratio_payroll >= UMBRAL_LO & ratio_payroll <= UMBRAL_HI,
    mpayroll_ajustada = ifelse(aplicar_payroll,
                               jun_payroll - 0.5 * abs(base_mediana_payroll),
                               jun_payroll)
  ) %>%
  select(numero_de_cliente, foto_mes,
         mcuentas_saldo_ajustada, mpayroll_ajustada)

# 4) Reemplazar SOLO en junio en el dataset original; no se agregan flags
dataset <- dataset %>%
  left_join(jun_adj, by = c("numero_de_cliente","foto_mes")) %>%
  mutate(
    mcuentas_saldo = ifelse(foto_mes == "202106" & !is.na(mcuentas_saldo_ajustada),
                            mcuentas_saldo_ajustada, mcuentas_saldo),
    mpayroll       = ifelse(foto_mes == "202106" & !is.na(mpayroll_ajustada),
                            mpayroll_ajustada, mpayroll)
  ) %>%
  select(-mcuentas_saldo_ajustada, -mpayroll_ajustada)

resumenes_agui_2_vars <- dataset %>% 
  group_by(foto_mes) %>% 
  summarise(mean(mpayroll),
            mean(mpayroll2),
            mean(mcuentas_saldo))

## calculo percentil  ######################
# -1 a 1 sobre vars financieras ##

vars_financieras  <- 
  c("mrentabilidad"
    ,"mrentabilidad_annual"
    ,"mcomisiones"
    ,"mactivos_margen"
    ,"mpasivos_margen"
    ,"mcuenta_corriente_adicional"
    ,"mcuenta_corriente"
    ,"mcaja_ahorro"
    ,"mcaja_ahorro_adicional"
    ,"mcaja_ahorro_dolares"
    ,"mcuentas_saldo"
    ,"mautoservicio"
    ,"mtarjeta_visa_consumo"
    ,"mtarjeta_master_consumo"
    ,"mprestamos_personales"
    ,"mprestamos_prendarios"
    ,"mprestamos_hipotecarios"
    ,"mplazo_fijo_dolares"
    ,"mplazo_fijo_pesos"
    ,"minversion1_pesos"
    ,"minversion1_dolares"
    ,"minversion2"
    ,"mpayroll"
    ,"mpayroll2"
    ,"mcuenta_debitos_automaticos"
    ,"mttarjeta_visa_debitos_automaticos"
    ,"mttarjeta_master_debitos_automaticos"
    ,"mpagodeservicios"
    ,"mpagomiscuentas"
    ,"mcajeros_propios_descuentos"
    ,"mtarjeta_visa_descuentos"
    ,"mtarjeta_master_descuentos"
    ,"mcomisiones_mantenimiento"
    ,"mcomisiones_otras"
    ,"mforex_buy"
    ,"mforex_sell"
    ,"mtransferencias_recibidas"
    ,"mtransferencias_emitidas"
    ,"mextraccion_autoservicio"
    ,"mcheques_depositados"
    ,"mcheques_emitidos"
    ,"mcheques_depositados_rechazados"
    ,"mcheques_emitidos_rechazados"
    ,"matm"
    ,"matm_other"
    ,"Master_mfinanciacion_limite"
    ,"Master_msaldototal"
    ,"Master_msaldopesos"
    ,"Master_msaldodolares"
    ,"Master_mconsumospesos"
    ,"Master_mconsumosdolares"
    ,"Master_mlimitecompra"
    ,"Master_madelantopesos"
    ,"Master_madelantodolares"
    ,"Master_mpagado"
    ,"Master_mpagospesos"
    ,"Master_mpagosdolares"
    ,"Master_mconsumototal"
    ,"Master_mpagominimo"
    ,"Visa_mfinanciacion_limite"
    ,"Visa_msaldototal"
    ,"Visa_msaldopesos"
    ,"Visa_msaldodolares"
    ,"Visa_mconsumospesos"
    ,"Visa_mconsumosdolares"
    ,"Visa_mlimitecompra"
    ,"Visa_madelantopesos"
    ,"Visa_madelantodolares"
    ,"Visa_mpagado"
    ,"Visa_mpagospesos"
    ,"Visa_mpagosdolares"
    ,"Visa_mconsumototal"
    ,"Visa_mpagominimo"
  )


# Percentil continuo con "cero fijo" por grupo:
# - Toma el percentil de |x| entre los NO-cero del grupo
# - Lo firma con sign(x)
# - 0 real mapea exactamente a 0
sZ_from_x <- function(x) {
  out <- rep(NA_real_, length(x))
  nz  <- is.finite(x) & x != 0    # no-cero válidos
  z0  <- is.finite(x) & x == 0    # ceros reales
  
  out[z0] <- 0
  m <- sum(nz)
  if (m > 0) {
    # rank de |x| entre no-cero (promedia empates)
    r <- rank(abs(x[nz]), ties.method = "average")
    # percentil 0..1 (maneja el caso m==1)
    pr <- if (m > 1) (r - 1) / (m - 1) else rep(1, m)
    out[nz] <- sign(x[nz]) * pr
  }
  out
}

library(tidyverse)

# transformacion
dataset <- dataset %>%
  group_by(foto_mes) %>%
  mutate(across(all_of(vars_financieras), sZ_from_x, .names = "sZ_{.col}")) %>%
  ungroup()

# dropear las vars anteriores
dataset <- dataset %>%
  select(-all_of(vars_financieras))


## calculo de lags y delta lags #########################
setDT(dataset)
setkey(dataset, numero_de_cliente, foto_mes)  # asegura orden temporal dentro de cada cliente

# solo numéricas, excluyendo IDs/llaves
num_cols <- names(dataset)[vapply(dataset, is.numeric, TRUE)]
num_cols <- setdiff(num_cols, c("numero_de_cliente", "foto_mes"))

# generar para lag = 1 y 2
lags <- c(1L, 2L)

for (L in lags) {
  # LAG L
  lag_cols_L <- paste0(num_cols, "_lag", L)
  dataset[, (lag_cols_L) := lapply(.SD, shift, n = L),
          by = numero_de_cliente, .SDcols = num_cols]
  
  # DELTA L (valor actual - valor de hace L meses)
  delta_cols_L <- paste0(num_cols, "_delta", L)
  dataset[, (delta_cols_L) := Map(`-`, mget(num_cols), mget(lag_cols_L))]
}


# cargo las librerias que necesito
require("data.table")
require("parallel")

if(!require("R.utils")) install.packages("R.utils")
require("R.utils")

if( !require("primes") ) install.packages("primes")
require("primes")

if( !require("utils") ) install.packages("utils")
require("utils")

if( !require("rlist") ) install.packages("rlist")
require("rlist")

if( !require("yaml")) install.packages("yaml")
require("yaml")

if( !require("lightgbm") ) install.packages("lightgbm")
require("lightgbm")

if( !require("DiceKriging") ) install.packages("DiceKriging")
require("DiceKriging")

if( !require("mlrMBO") ) install.packages("mlrMBO")
require("mlrMBO")

# para BO inteligentes
library(lhs)

# Parametros bayesiana ###################
PARAM <- list()
PARAM$experimento <- 10004   ########### cambiar experimentooooooooooo!
PARAM$semilla_primigenia <- 991027

# training y future
PARAM$train <- c(202101, 202102, 202103, 202104)
PARAM$train_final <- c(202101, 202102, 202103, 202104)
PARAM$future <- c(202106)
PARAM$semilla_kaggle <- 314159
PARAM$cortes <- seq(6000, 19000, by= 500)

# un undersampling de 0.1  toma solo el 10% de los CONTINUA
# undersampling de 1.0  implica tomar TODOS los datos

PARAM$trainingstrategy$undersampling <- 0.25

# Parametros LightGBM

PARAM$hyperparametertuning$xval_folds <- 5

# parametros fijos del LightGBM que se pisaran con la parte variable de la BO
PARAM$lgbm$param_fijos <-  list(
  boosting= "gbdt", # puede ir  dart  , ni pruebe random_forest
  objective= "binary",
  metric= "auc",
  first_metric_only= FALSE,
  boost_from_average= TRUE,
  feature_pre_filter= FALSE,
  force_row_wise= TRUE, # para reducir warnings
  verbosity= -100,
  
  seed= PARAM$semilla_primigenia,
  
  max_depth= -1L, # -1 significa no limitar,  por ahora lo dejo fijo
  min_gain_to_split= 0, # min_gain_to_split >= 0
  min_sum_hessian_in_leaf= 0.001, #  min_sum_hessian_in_leaf >= 0.0
  lambda_l1= 0.0, # lambda_l1 >= 0.0
  lambda_l2= 0.0, # lambda_l2 >= 0.0
  max_bin= 31L, # lo debo dejar fijo, no participa de la BO
  
  bagging_fraction= 1.0, # 0.0 < bagging_fraction <= 1.0
  pos_bagging_fraction= 1.0, # 0.0 < pos_bagging_fraction <= 1.0
  neg_bagging_fraction= 1.0, # 0.0 < neg_bagging_fraction <= 1.0
  is_unbalance= FALSE, #
  scale_pos_weight= 1.0, # scale_pos_weight > 0.0
  
  drop_rate= 0.1, # 0.0 < neg_bagging_fraction <= 1.0
  max_drop= 50, # <=0 means no limit
  skip_drop= 0.5, # 0.0 <= skip_drop <= 1.0
  
  extra_trees= FALSE,
  
  num_iterations= 1200,
  learning_rate= 0.02,
  feature_fraction= 0.5,
  num_leaves= 750,
  min_data_in_leaf= 5000
)

# Aqui se cargan los bordes de los hiperparametros de la BO
PARAM$hypeparametertuning$hs <- makeParamSet(
  makeIntegerParam("num_iterations", lower= 8L, upper= 2048L),
  makeNumericParam("learning_rate", lower= 0.01, upper= 0.3),
  makeNumericParam("feature_fraction", lower= 0.1, upper= 1.0),
  makeIntegerParam("num_leaves", lower= 8L, upper= 2048L),
  makeIntegerParam("min_data_in_leaf", lower= 1L, upper= 5000L)
)

PARAM$hyperparametertuning$iteraciones <- 50 # iteraciones bayesianas ||||| ojo esto no se aplica luego!!! estan las "inteligentes"

# particionar agrega una columna llamada fold a un dataset
#   que consiste en una particion estratificada segun agrupa
# particionar( data=dataset, division=c(70,30),
#  agrupa=clase_ternaria, seed=semilla)   crea una particion 70, 30

particionar <- function(data, division, agrupa= "", campo= "fold", start= 1, seed= NA) {
  if (!is.na(seed)) set.seed(seed, "L'Ecuyer-CMRG")
  
  bloque <- unlist(mapply(
    function(x, y) {rep(y, x)},division, seq(from= start, length.out= length(division))))
  
  data[, (campo) := sample(rep(bloque,ceiling(.N / length(bloque))))[1:.N],by= agrupa]
}

# iniciliazo el dataset de realidad, para medir ganancia
realidad_inicializar <- function( pfuture, pparam) {
  
  # datos para verificar la ganancia
  drealidad <- pfuture[, list(numero_de_cliente, foto_mes, clase_ternaria)]
  
  particionar(drealidad,
              division= c(3, 7),
              agrupa= "clase_ternaria",
              seed= PARAM$semilla_kaggle
  )
  
  return( drealidad )
}

# evaluo ganancia en los datos de la realidad

realidad_evaluar <- function( prealidad, pprediccion) {
  
  prealidad[ pprediccion,
             on= c("numero_de_cliente", "foto_mes"),
             predicted:= i.Predicted
  ]
  
  tbl <- prealidad[, list("qty"=.N), list(fold, predicted, clase_ternaria)]
  
  res <- list()
  res$public  <- tbl[fold==1 & predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]/0.3
  res$private <- tbl[fold==2 & predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]/0.7
  res$total <- tbl[predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]
  
  prealidad[, predicted:=NULL]
  return( res )
}

# carpeta de trabajo

# setwd("..")
experimento_folder <- paste0("./buckets/b1/HT", PARAM$experimento)
dir.create(experimento_folder, showWarnings=FALSE)
# setwd( file.path(experimento_folder ))

dataset_train <- dataset[foto_mes %in% PARAM$train]


# paso la clase a binaria que tome valores {0,1}  enteros
#  BAJA+1 y BAJA+2  son  1,   CONTINUA es 0
#  a partir de ahora ya NO puedo cortar  por prob(BAJA+2) > 1/40

dataset_train[,
              clase01 := ifelse(clase_ternaria %in% c("BAJA+2","BAJA+1"), 1L, 0L)
]

# defino los datos que forma parte del training
# aqui se hace el undersampling de los CONTINUA
# notar que para esto utilizo la SEGUNDA semilla

set.seed(PARAM$semilla_primigenia, kind = "L'Ecuyer-CMRG")
dataset_train[, azar := runif(nrow(dataset_train))]
dataset_train[, training := 0L]

dataset_train[
  foto_mes %in%  PARAM$train &
    (azar <= PARAM$trainingstrategy$undersampling | clase_ternaria %in% c("BAJA+1", "BAJA+2")),
  training := 1L
]

# los campos que se van a utilizar
# todos menos los c()
campos_buenos <- setdiff(
  colnames(dataset_train),
  c("clase_ternaria", "clase01", "azar", "training")
)

# dejo los datos en el formato que necesita LightGBM

dtrain <- lgb.Dataset(
  data= data.matrix(dataset_train[training == 1L, campos_buenos, with= FALSE]),
  label= dataset_train[training == 1L, clase01],
  free_raw_data= FALSE
)

nrow(dtrain)
ncol(dtrain)


# En el argumento x llegan los parmaetros de la bayesiana
#  devuelve la AUC en cross validation del modelo entrenado

EstimarGanancia_AUC_lightgbm <- function(x) {
  
  # x pisa (o agrega) a param_fijos
  param_completo <- modifyList(PARAM$lgbm$param_fijos, x)
  
  # entreno LightGBM
  modelocv <- lgb.cv(
    data= dtrain,
    nfold= PARAM$hyperparametertuning$xval_folds,
    stratified= TRUE,
    param= param_completo
  )
  
  # obtengo la ganancia
  AUC <- modelocv$best_score
  
  # hago espacio en la memoria
  rm(modelocv)
  gc(full= TRUE, verbose= FALSE)
  
  message(format(Sys.time(), "%a %b %d %X %Y"), " AUC ", AUC)
  
  return(AUC)
}

# Aqui comienza la configuracion de la Bayesian Optimization

# en este archivo quedan la evolucion binaria de la BO
kbayesiana <- paste0("./buckets/b1/HT", PARAM$experimento, "/bayesiana.RDATA")

funcion_optimizar <- EstimarGanancia_AUC_lightgbm # la funcion que voy a maximizar

configureMlr(show.learner.output= FALSE)

# configuro la busqueda bayesiana,  los hiperparametros que se van a optimizar
# por favor, no desesperarse por lo complejo

obj.fun <- makeSingleObjectiveFunction(
  fn= funcion_optimizar, # la funcion que voy a maximizar
  minimize= FALSE, # estoy Maximizando la ganancia
  noisy= TRUE,
  par.set= PARAM$hypeparametertuning$hs, # definido al comienzo del programa
  has.simple.signature= FALSE # paso los parametros en una lista
)

### no inteligentes > BO no inteligente
design_init <- generateDesign(
  n       = 20,
  par.set = PARAM$hypeparametertuning$hs,
  fun     = lhs::maximinLHS
)



# cada 600 segundos guardo el resultado intermedio
ctrl <- makeMBOControl(
  save.on.disk.at.time= 600, # se graba cada 600 segundos
  save.file.path= kbayesiana,
  save.on.disk.at = c(0, 1:PARAM$hyperparametertuning$iteraciones, 
                      PARAM$hyperparametertuning$iteraciones + 1)
) # se graba cada 600 segundos

# indico la cantidad de iteraciones que va a tener la Bayesian Optimization
ctrl <- setMBOControlTermination(
  ctrl,
  # iters= PARAM$hyperparametertuning$iteraciones
  iters = 30
) # cantidad de iteraciones

# defino el método estandar para la creacion de los puntos iniciales,
# los "No Inteligentes"
ctrl <- setMBOControlInfill(ctrl, crit= makeMBOInfillCritEI())

# establezco la funcion que busca el maximo
surr.km <- makeLearner(
  "regr.km",
  predict.type= "se",
  covtype= "matern3_2",
  control= list(trace= TRUE)
)

# inicio la optimizacion bayesiana, retomando si ya existe
# es la celda mas lenta de todo el notebook

if (!file.exists(kbayesiana)) {
  bayesiana_salida <- mbo(obj.fun, 
                          learner= surr.km, 
                          control= ctrl,
                          design  = design_init ### >>>> implemento la BO inteligente
                          )
} else {
  bayesiana_salida <- mboContinue(kbayesiana) # retomo en caso que ya exista
}



tb_bayesiana <- as.data.table(bayesiana_salida$opt.path)
colnames( tb_bayesiana)

# almaceno los resultados de la Bayesian Optimization
# y capturo los mejores hiperparametros encontrados

tb_bayesiana <- as.data.table(bayesiana_salida$opt.path)

tb_bayesiana[, iter := .I]

# ordeno en forma descendente por AUC = y
setorder(tb_bayesiana, -y)

# grabo para eventualmente poder utilizarlos en OTRA corrida
fwrite( tb_bayesiana,
        file= paste0("./buckets/b1/HT", PARAM$experimento, "/BO_log.txt"),
        sep= "\t"
)

# los mejores hiperparámetros son los que quedaron en el registro 1 de la tabla
PARAM$out$lgbm$mejores_hiperparametros <- tb_bayesiana[
  1, # el primero es el de mejor AUC
  setdiff(colnames(tb_bayesiana),
          c("y","dob","eol","error.message","exec.time","ei","error.model",
            "train.time","prop.type","propose.time","se","mean","iter")),
  with= FALSE
]


PARAM$out$lgbm$y <- tb_bayesiana[1, y]

write_yaml( PARAM, file= paste0("./buckets/b1/HT", PARAM$experimento, "/PARAM.yml")
)



print(PARAM$out$lgbm$mejores_hiperparametros)
print(PARAM$out$lgbm$y)

# setwd("./exp")
experimento <- paste0("./buckets/b1/", "exp", PARAM$experimento)
dir.create(experimento, showWarnings= FALSE)
# setwd( paste0("./exp/", experimento ))

# clase01
dataset[, clase01 := ifelse(clase_ternaria %in% c("BAJA+1", "BAJA+2"), 1L, 0L)]

dataset_train <- dataset[foto_mes %in% PARAM$train_final]
dataset_train[,.N,clase_ternaria] # simple count 

# dejo los datos en el formato que necesita LightGBM

dtrain_final <- lgb.Dataset(
  data= data.matrix(dataset_train[, campos_buenos, with= FALSE]),
  label= dataset_train[, clase01]
)

param_final <- modifyList(PARAM$lgbm$param_fijos,
                          PARAM$out$lgbm$mejores_hiperparametros)

param_final

# este punto es muy SUTIL  y será revisado en la Clase 05

param_normalizado <- copy(param_final)
param_normalizado$min_data_in_leaf <-  round(param_final$min_data_in_leaf / PARAM$trainingstrategy$undersampling)

# # entreno LightGBM
# 
# modelo_final <- lgb.train(
#   data= dtrain_final,
#   param= param_normalizado
# )
# 
# # ahora imprimo la importancia de variables
# 
# tb_importancia <- as.data.table(lgb.importance(modelo_final))
# # archivo_importancia <- "impo.txt"
# archivo_importancia <- paste0("./buckets/b1/", "exp", PARAM$experimento, "/impo.txt")
# lgb.plot.importance(tb_importancia, top_n = 30, measure = "Gain")
# 
# ver_variables <- 20  # poné el número que quieras; usa nrow(tb_importancia) para “todas”
# plot_dt <- copy(tb_importancia)[order(-Gain)][1:min(ver_variables, .N)]
# ggplot(plot_dt, aes(x = reorder(Feature, Gain), y = Gain)) +
#   geom_col() +
#   coord_flip() +
#   labs(x = NULL, y = "Gain", title = paste("Importancia (Top", nrow(plot_dt), ")")) +
#   theme_minimal()
# 
# match("numero_de_cliente", tb_importancia[order(-Gain), Feature])
# 
# 
# fwrite(tb_importancia,
#        file= archivo_importancia,
#        sep= "\t"
# )
# 
# # grabo a disco el modelo en un formato para seres humanos ... ponele ...
# # lgb.save(modelo_final, "modelo.txt" )
# lgb.save(modelo_final, paste0("./buckets/b1/", "exp", PARAM$experimento, "/modelo.txt"))
# 
# # aplico el modelo a los datos sin clase
# dfuture <- dataset[foto_mes %in% PARAM$future]
# 
# # aplico el modelo a los datos nuevos
# prediccion <- predict(
#   modelo_final,
#   data.matrix(dfuture[, campos_buenos, with= FALSE])
# )
# 
# # inicilizo el dataset  drealidad
# drealidad <- realidad_inicializar( dfuture, PARAM)
# 
# # tabla de prediccion
# 
# tb_prediccion <- dfuture[, list(numero_de_cliente, foto_mes)]
# tb_prediccion[, prob := prediccion ]
# 
# # grabo las probabilidad del modelo
# fwrite(tb_prediccion,
#        file= paste0("./buckets/b1/", "exp", PARAM$experimento, "/prediccion.txt"),
#        sep= "\t"
# )
# 
# PARAM$cortes
# 
# # genero archivos con los  "envios" mejores
# # suba TODOS los archivos a Kaggle
# 
# # ordeno por probabilidad descendente
# setorder(tb_prediccion, -prob)
# 
# dir.create(paste0("./buckets/b1/", "exp", PARAM$experimento, "/kaggle"))
# 
# for (envios in PARAM$cortes) {
#   
#   tb_prediccion[, Predicted := 0L] # seteo inicial a 0
#   tb_prediccion[1:envios, Predicted := 1L] # marco los primeros
#   
#   archivo_kaggle <- paste0("./buckets/b1/", "exp", PARAM$experimento, "/kaggle", PARAM$experimento, "_", envios, ".csv")
#   
#   # grabo el archivo
#   fwrite(tb_prediccion[, list(numero_de_cliente, Predicted)],
#          file= archivo_kaggle,
#          sep= ","
#   )
#   
#   res <- realidad_evaluar( drealidad, tb_prediccion)
#   
#   options(scipen = 999)
#   cat( "Envios=", envios, "\t",
#        " TOTAL=", res$total,
#        "  Public=", res$public,
#        " Private=", res$private,
#        "\n",
#        sep= ""
#   )
#   
# }

write_yaml( PARAM, file="PARAM.yml")

format(Sys.time(), "%a %b %d %X %Y")

##### Metodo ensamble de predicciones ########
# primero probamos con 5 iteraciones de modelos
X_future <- data.matrix(dfuture)

param_base <- param_normalizado
param_base$feature_fraction <- min(0.9, param_base$feature_fraction %||% 0.9)
param_base$bagging_fraction <- min(0.9, param_base$bagging_fraction %||% 0.9)
param_base$bagging_freq     <- 1L

seeds <- c(991027, 991031, 991037, 991043, 991057)

preds <- matrix(NA_real_, nrow = nrow(X_future), ncol = length(seeds))

# 4) Loop: re-hago el undersampling por seed, entreno y predigo
for (i in seq_along(seeds)) {
  s <- seeds[i]
  param_i <- param_base
  param_i$seed <- s
  
  # Rehacer muestra de entrenamiento por seed
  set.seed(s, kind = "L'Ecuyer-CMRG")
  dataset_train[, azar := runif(.N)]
  dataset_train[, training := 0L]
  dataset_train[
    foto_mes %in% PARAM$train &
      (azar <= PARAM$trainingstrategy$undersampling | clase_ternaria %in% c("BAJA+1","BAJA+2")),
    training := 1L
  ]
  
  # dtrain de esta seed (MATRIZ!)
  X_train <- data.matrix(dataset_train[training == 1L, campos_buenos, with = FALSE])
  y_train <- dataset_train[training == 1L, clase01]
  
  dtrain_seed <- lgb.Dataset(
    data = X_train,
    label = y_train,
    free_raw_data = FALSE
  )
  
  # Entrenar
  modelos_i <- lgb.train(
    params  = param_i,
    data    = dtrain_seed,
    nrounds = param_i$num_iterations 
    # si querés early_stopping: agregá valid_sets y early_stopping_rounds
  )
  
  # Predecir (siempre sobre la MISMA matriz X_future)
  # preds[, i] <- predict(modelos_i, data.matrix(X_future[, campos_buenos, with= FALSE])
  
  preds[, i] <- predict(modelos_i, X_future[, match(campos_buenos, colnames(X_future)), drop = FALSE]
                        )
}


# 5) Ensamble
proba_avg <- rowMeans(preds)

# 6) Top-N por cortes (ranking robusto)
tb_prediccion <- dfuture[, .(numero_de_cliente, foto_mes)]
tb_prediccion[, prob := proba_avg]
tb_prediccion[, rk := frank(-prob, ties.method = "first")]


dir.create(file.path("./buckets/b1", paste0("exp", PARAM$experimento), "kaggle_ensamble"), showWarnings = FALSE)
for (envios in PARAM$cortes) {
  tb_prediccion[, Predicted := 0L]           # pone 0 en todos
  tb_prediccion[rk <= envios, Predicted := 1L]  # marca 1 solo a los Top-N
  fwrite(tb_prediccion[, .(numero_de_cliente, Predicted)],
         file = file.path("./buckets/b1", paste0("exp", PARAM$experimento), "kaggle_ensamble",
                          paste0(PARAM$experimento, "_", envios, ".csv")),
         sep = ",")
  print(envios)
}


# Probamos con 10 iteraciones de modelos y ajuste de algunos HP 
# deberia haberlos optimizado en la bayesiana

# matriz de futuro (ya tenés dfuture armado y campos_buenos definidos/ordenados)
X_future <- data.matrix(dfuture)

param_base <- param_normalizado
param_base$feature_fraction <- min(0.9, param_base$feature_fraction %||% 0.9)
param_base$bagging_fraction <- min(0.9, param_base$bagging_fraction %||% 0.9)
param_base$bagging_freq     <- 1L
param_base$extra_trees      <- FALSE

# 10 seeds
seeds <- c(991027, 991031, 991037, 991043, 991057, 314159, 271828, 161803, 123457, 424243)

preds <- matrix(NA_real_, nrow = nrow(X_future), ncol = length(seeds))

for (i in seq_along(seeds)) {
  s <- seeds[i]
  param_i <- param_base
  param_i$seed <- s
  
  # --- re-muestreo por seed (undersampling) ---
  set.seed(s, kind = "L'Ecuyer-CMRG")
  dataset_train[, azar := runif(.N)]
  dataset_train[, training := 0L]
  dataset_train[
    foto_mes %in% PARAM$train &
      (azar <= PARAM$trainingstrategy$undersampling | clase_ternaria %in% c("BAJA+1","BAJA+2")),
    training := 1L
  ]
  
  X_train <- data.matrix(dataset_train[training == 1L, campos_buenos, with = FALSE])
  y_train <- dataset_train[training == 1L, clase01]
  dtrain_seed <- lgb.Dataset(data = X_train, label = y_train, free_raw_data = FALSE)
  
  # --- JITTER LEVE por seed (diversidad controlada) ---
  jitter01 <- function(v, lo=0.90, hi=1.05, vmin=NULL, vmax=NULL) {
    out <- v * runif(1, lo, hi)
    if (!is.null(vmin)) out <- max(vmin, out)
    if (!is.null(vmax)) out <- min(vmax, out)
    out
  }
  
  param_i$feature_fraction <- jitter01(param_i$feature_fraction, 0.90, 1.05, 0.20, 0.95)
  param_i$bagging_fraction <- jitter01(param_i$bagging_fraction, 0.90, 1.05, 0.20, 0.95)
  param_i$num_leaves       <- max(8L, as.integer(jitter01(param_i$num_leaves, 0.90, 1.10)))
  param_i$min_data_in_leaf <- max(4L, as.integer(jitter01(param_i$min_data_in_leaf, 1.00, 1.50)))
  
  # regularización suave (si venían en 0/NULL, introduce un pequeño valor)
  if (is.null(param_i$lambda_l1) || param_i$lambda_l1 == 0) {
    param_i$lambda_l1 <- runif(1, 0, 0.1)
  } else {
    param_i$lambda_l1 <- jitter01(param_i$lambda_l1, 0.8, 1.2)
  }
  if (is.null(param_i$lambda_l2) || param_i$lambda_l2 == 0) {
    param_i$lambda_l2 <- runif(1, 0, 0.1)
  } else {
    param_i$lambda_l2 <- jitter01(param_i$lambda_l2, 0.8, 1.2)
  }
  
  # diversidad adicional: algunos extra_trees y pocos DART
  if (i %% 7 == 0) {
    param_i$boosting  <- "dart"
    param_i$drop_rate <- 0.10
    param_i$max_drop  <- 50
    param_i$skip_drop <- 0.5
  } else if (i %% 5 == 0) {
    param_i$extra_trees <- TRUE
    param_i$boosting    <- "gbdt"
  } else {
    param_i$boosting    <- "gbdt"
  }
  
  # Entrenar
  modelos_i <- lgb.train(
    params  = param_i,
    data    = dtrain_seed,
    nrounds = param_i$num_iterations
  )
  
  print('entrenado: ', i)
  
  # Predecir (alineado a campos_buenos)
  preds[, i] <- predict(
    modelos_i,
    X_future[, match(campos_buenos, colnames(X_future)), drop = FALSE]
  )
  
  print('prediccion: ', i)
}

# Ensamble
proba_avg <- rowMeans(preds)

# Ranking y submits (Top-N por cortes)
tb_prediccion <- dfuture[, .(numero_de_cliente, foto_mes)]
tb_prediccion[, prob := proba_avg]
tb_prediccion[, rk := frank(-prob, ties.method = "first")]

dir.create(file.path("./buckets/b1", paste0("exp", PARAM$experimento), "kaggle_ensamble_10"),
           showWarnings = FALSE)

for (envios in PARAM$cortes) {
  tb_prediccion[, Predicted := 0L]
  tb_prediccion[rk <= envios, Predicted := 1L]
  fwrite(tb_prediccion[, .(numero_de_cliente, Predicted)],
         file = file.path("./buckets/b1", paste0("exp", PARAM$experimento), "kaggle_ensamble_10",
                          paste0(PARAM$experimento, "_", envios, ".csv")),
         sep = ",")
  print(envios)
}

# No tengo método de validación interna de los resultados.
# Deberia reentrenar BO 01 a 03 y realizar los test en 04 con los hp ampliados
# Selecciono predicción a entregar en base a lo visto colaborativamente (envios ~ 10000~11000)


