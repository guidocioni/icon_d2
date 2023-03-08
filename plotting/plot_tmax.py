import numpy as np
from multiprocessing import Pool
from functools import partial
import utils
import sys

debug = False
if not debug:
    import matplotlib
    matplotlib.use('Agg')

import matplotlib.pyplot as plt

# The one employed for the figure name when exported 
variable_name = 'tmax'

utils.print_message('Starting script to plot '+variable_name)

# Get the projection as system argument from the call so that we can
# span multiple instances of this script outside
if not sys.argv[1:]:
    utils.print_message(
        'Projection not defined, falling back to default (euratl)')
    projection = 'de'
else:
    projection = sys.argv[1]


def main():
    """In the main function we basically read the files and prepare the variables to be plotted.
    This is not included in utils.py as it can change from case to case."""
    dset = utils.read_dataset(variables=['tmax_2m'], projection=projection)
    dset['TMAX_2M'] = dset['TMAX_2M'].metpy.convert_units('degC').metpy.dequantify()

    levels_t2m = np.arange(-25, 50, 1)

    cmap = utils.get_colormap("temp")

    _ = plt.figure(figsize=(utils.figsize_x, utils.figsize_y))

    ax  = plt.gca()
    m, x, y = utils.get_projection(dset, projection, labels=True)

    dset = dset.load()

    # All the arguments that need to be passed to the plotting function
    args=dict(x=x, y=y, ax=ax, cmap=cmap,
             levels_t2m=levels_t2m,
             time=dset.time)

    utils.print_message('Pre-processing finished, launching plotting scripts')
    if debug:
        plot_files(dset.isel(time=slice(0, 2)), **args)
    else:
        # Parallelize the plotting by dividing into chunks and utils.processes
        dss = utils.chunks_dataset(dset, utils.chunks_size)
        plot_files_param = partial(plot_files, **args)
        p = Pool(utils.processes)
        p.map(plot_files_param, dss)


def plot_files(dss, **args):
    first = True
    for time_sel in dss.time:
        data = dss.sel(time=time_sel)
        time, run, cum_hour = utils.get_time_run_cum(data)
        # Build the name of the output image
        filename = utils.subfolder_images[projection] + '/' + variable_name + '_%s.png' % cum_hour

        cs = args['ax'].contourf(args['x'], args['y'],
                                 data['TMAX_2M'],
                                 extend='both',
                                 cmap=args['cmap'],
                                 levels=args['levels_t2m'])

        # plot every -th element
        if projection=="nord":
            density = 9
        elif projection=="it":
            density = 11
        elif projection=="de":
            density = 15
        else:
            density = 22

        vals = utils.add_vals_on_map(args['ax'], projection,
                               data['TMAX_2M'], args['levels_t2m'],
                               cmap=args['cmap'],
                               density=density)

        an_fc = utils.annotation_forecast(args['ax'], time)
        an_var = utils.annotation(args['ax'], 'Maximum 2m Temperature in previous 6 hours',
                            loc='lower left', fontsize=6)
        an_run = utils.annotation_run(args['ax'], run)
        

        if first:
            plt.colorbar(cs, orientation='horizontal', label='Temperature [C]', pad=0.03, fraction=0.04)
        
        if debug:
            plt.show(block=True)
        else:
            plt.savefig(filename, **utils.options_savefig)        

        utils.remove_collections([cs, an_fc, an_var, an_run, vals])

        first = False


if __name__ == "__main__":
    import time
    start_time=time.time()
    main()
    elapsed_time=time.time()-start_time
    utils.print_message("script took " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
