import numpy as np
from multiprocessing import Pool
from functools import partial
from utils import *
import sys

debug = False
if not debug:
    import matplotlib
    matplotlib.use('Agg')

import matplotlib.pyplot as plt

# The one employed for the figure name when exported
variable_name = 'cape_cin'

print_message('Starting script to plot ' + variable_name)

# Get the projection as system argument from the call so that we can
# span multiple instances of this script outside
if not sys.argv[1:]:
    print_message(
        'Projection not defined, falling back to default (de)')
    projection = 'de'
else:
    projection = sys.argv[1]


def main():
    dset = read_dataset(variables=['cape_ml', 'cin_ml', 'u', 'v'],
                        projection=projection,
                        level=85000)

    dset['CAPE_ML'] = dset['CAPE_ML'].where(dset['CAPE_ML'] >= 250)
    dset['CIN_ML'] = dset['CIN_ML'].where(((dset['CIN_ML'] < 0) & (dset['CIN_ML'] > -150)))

    levels_cape = np.arange(250., 5000., 50.)
    cmap = get_colormap("winds")

    # initialize figure
    _ = plt.figure(figsize=(figsize_x, figsize_y))
    ax = plt.gca()
    # Get coordinates from dataset
    m, x, y = get_projection(dset, projection, labels=True)
    # additional maps adjustment for this map
    m.fillcontinents(color='lightgray', lake_color='whitesmoke', zorder=0)

    dset = dset.drop(['lon', 'lat']).load()

    # All the arguments that need to be passed to the plotting function
    # we pass only arrays to avoid the pickle problem when unpacking in multiprocessing
    args = dict(x=x, y=y, ax=ax, cmap=cmap,
                levels_cape=levels_cape)

    print_message('Pre-processing finished, launching plotting scripts')
    if debug:
        plot_files(dset.isel(time=slice(0, 2)), **args)
    else:
        # Parallelize the plotting by dividing into chunks and processes
        dss = chunks_dataset(dset, chunks_size)
        plot_files_param = partial(plot_files, **args)
        p = Pool(processes)
        p.map(plot_files_param, dss)


def plot_files(dss, **args):
    first = True
    for time_sel in dss.time:
        data = dss.sel(time=time_sel)
        time, run, cum_hour = get_time_run_cum(data)
        # Build the name of the output image
        filename = subfolder_images[projection] + '/' + variable_name + '_%s.png' % cum_hour

        cs = args['ax'].contourf(args['x'], args['y'],
                                 data['CAPE_ML'],
                                 extend='max',
                                 cmap=args['cmap'],
                                 levels=args['levels_cape'])
        cr = args['ax'].contourf(args['x'], args['y'],
                                 data['CIN_ML'],
                                 colors='none',
                                 levels=(-100., -50.),
                                 hatches=['...', '...'])


        density = 15
        cv = args['ax'].quiver(args['x'][::density, ::density],
                               args['y'][::density, ::density],
                               data['u'][::density, ::density],
                               data['v'][::density, ::density],
                               scale=None,
                               alpha=0.8, color='gray')

        an_fc = annotation_forecast(args['ax'], time)
        an_var = annotation(args['ax'], 'CAPE and Winds@850 hPa, hatches CIN$<-50$ J/kg',
            loc='lower left', fontsize=6)
        an_run = annotation_run(args['ax'], run)
        logo = add_logo_on_map(ax=args['ax'],
                                zoom=0.1, pos=(0.95, 0.08))

        if first:
            plt.colorbar(cs, orientation='horizontal', label='CAPE [J/kg]', pad=0.04, fraction=0.04)

        if debug:
            plt.show(block=True)
        else:
            plt.savefig(filename, **options_savefig)        

        remove_collections([cs, an_fc, an_var, an_run, cv, cr, logo])

        first = False 

if __name__ == "__main__":
    import time
    start_time=time.time()
    main()
    elapsed_time=time.time()-start_time
    print_message("script took " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
