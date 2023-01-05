import matplotlib.pyplot as plt
import numpy as np
from multiprocessing import Pool
from functools import partial
import utils*
import sys

debug = False
if not debug:
    import matplotlib
    matplotlib.use('Agg')


# The one employed for the figure name when exported
variable_name = 'cape_cin'

utils.print_message('Starting script to plot ' + variable_name)

# Get the projection as system argument from the call so that we can
# span multiple instances of this script outside
if not sys.argv[1:]:
    utils.print_message(
        'Projection not defined, falling back to default (de)')
    projection = 'de'
else:
    projection = sys.argv[1]


def main():
    dset = utils.read_dataset(variables=['cape_ml', 'cin_ml', 'u', 'v'],
                        projection=projection,
                        level=85000)

    dset['CAPE_ML'] = dset['CAPE_ML'].where(dset['CAPE_ML'] >= 100)
    # dset['CIN_ML'] = dset['CIN_ML'].where(
    #     ((dset['CIN_ML'] < 0) & (dset['CIN_ML'] > -150)))

    levels_cape = np.concatenate([np.arange(0., 3000., 100.),
                                 np.arange(3000., 7000., 200.)])
    cmap, norm = get_colormap_norm('cape_wxcharts', levels=levels_cape)

    # initialize figure
    _ = plt.figure(figsize=(utils.figsize_x, utils.figsize_y))
    ax = plt.gca()
    # Get coordinates from dataset
    m, x, y = utils.get_projection(dset, projection, labels=True)
    # additional maps adjustment for this map
    m.arcgisimage(service='World_Shaded_Relief', xpixels=1500)

    dset = dset.drop(['lon', 'lat']).load()

    # All the arguments that need to be passed to the plotting function
    # we pass only arrays to avoid the pickle problem when unpacking in multiprocessing
    args = dict(x=x, y=y, ax=ax, cmap=cmap, norm=norm,
                levels_cape=levels_cape)

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
        filename = utils.subfolder_images[projection] + \
            '/' + variable_name + '_%s.png' % cum_hour

        cs = args['ax'].contourf(args['x'], args['y'],
                                 data['CAPE_ML'],
                                 extend='max',
                                 cmap=args['cmap'],
                                 levels=args['levels_cape'])
        cr = args['ax'].contourf(args['x'], args['y'],
                                 data['CIN_ML'],
                                 colors='none',
                                 levels=(50, 100.),
                                 hatches=['...', '...'],
                                 zorder=5)

        density = 15
        cv = args['ax'].quiver(args['x'][::density, ::density],
                               args['y'][::density, ::density],
                               data['u'][::density, ::density],
                               data['v'][::density, ::density],
                               scale=None,
                               alpha=0.8, color='gray')

        an_fc = utils.utils.annotation_forecast(args['ax'], time)
        an_var = utils.annotation(args['ax'], 'CAPE and Winds@850 hPa, hatches CIN$<-50$ J/kg',
                            loc='lower left', fontsize=6)
        an_run = utils.annotation_run(args['ax'], run)
        

        if first:
            plt.colorbar(cs, orientation='horizontal',
                         label='CAPE [J/kg]', pad=0.04, fraction=0.04)

        if debug:
            plt.show(block=True)
        else:
            plt.savefig(filename, **utils.options_savefig)

        utils.remove_collections([cs, an_fc, an_var, an_run, cv, cr])

        first = False


if __name__ == "__main__":
    import time
    start_time = time.time()
    main()
    elapsed_time = time.time()-start_time
    utils.print_message("script took " + time.strftime("%H:%M:%S",
                  time.gmtime(elapsed_time)))
