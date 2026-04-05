add_library(
  micelio-simulation
  model/edge.cc
  model/fog.cc
  model/cloud.cc
  model/user.cc
)

set(Torch_DIR "$ENV{LIBTORCH}/share/cmake/Torch")

set(CMAKE_CUDA_ARCHITECTURES 86)
find_package(Torch REQUIRED)

build_exec(
  EXECNAME micelio
  SOURCE_FILES simulation_brite.cc
  LIBRARIES_TO_LINK dl
                    micelio
                    nsrs
                    micelio-simulation
                    ${TORCH_LIBRARIES}
                    ${libapplications}
                    ${libbrite}
                    ${libcore}
                    ${libcsma}
                    ${libinternet-apps}
                    ${libinternet}
                    ${liblte}
                    ${libmobility}
                    ${libpoint-to-point}
                    ${libwifi}

  EXECUTABLE_DIRECTORY_PATH ${CMAKE_OUTPUT_DIRECTORY}/scratch/micelio
)